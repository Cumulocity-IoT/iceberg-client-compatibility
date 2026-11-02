import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;

/** The checks of clients/CONTRACT.md: Checks <plan.json> <out.json>. */
public class Checks {

  interface Check {
    Map<String, Object> run(RESTCatalog catalog) throws Exception;
  }

  /** A check on the run's row of the fixture table. */
  interface RowCheck {
    Map<String, Object> run(RESTCatalog catalog, Table table, Record row) throws Exception;
  }

  static JsonNode plan;
  static RESTCatalog regionCatalog;

  public static void main(String[] args) throws Exception {
    ObjectMapper json = new ObjectMapper();
    plan = json.readTree(new File(args[0]));

    Map<String, Object> checks = new LinkedHashMap<>();
    RESTCatalog catalog = null;
    try {
      catalog = Example.connect(Map.of());
      checks.put("catalog-login", run(catalog, Checks::catalogLogin, false));
      checks.put("s3", run(catalog, Checks::s3, true));
      checks.put("cdc-tables", run(catalog, Checks::cdcTables, true));
      checks.put("latest-tables", run(catalog, Checks::latestTables, true));
      checks.put("identifiers", run(catalog, onRunRow(Checks::identifiers), true));
      checks.put("views", run(catalog, Checks::views, false));
      checks.put("decimal", run(catalog, onRunRow(Checks::decimal), true));
      TimeZone zone = TimeZone.getDefault();
      TimeZone.setDefault(TimeZone.getTimeZone("Asia/Kolkata"));
      try {
        checks.put("timestamps", run(catalog, onRunRow(Checks::timestamps), true));
      } finally {
        TimeZone.setDefault(zone);
      }
      checks.put("nesting", run(catalog, onRunRow(Checks::nesting), true));
      checks.put("large-values", run(catalog, onRunRow(Checks::largeValues), true));
      checks.put("schema-evolution", run(catalog, onRunRow(Checks::schemaEvolution), true));
      checks.put("incremental-read", run(catalog, Checks::incrementalRead, true));
    } catch (Exception e) {
      checks.put("catalog-login", fail(e));
      for (String id : List.of("s3", "cdc-tables", "latest-tables", "identifiers", "views", "decimal", "timestamps",
          "nesting", "large-values", "schema-evolution", "incremental-read")) {
        checks.put(id, result("inconclusive", "The catalog login failed.", null));
      }
    } finally {
      if (catalog != null) {
        catalog.close();
      }
      if (regionCatalog != null) {
        regionCatalog.close();
      }
    }

    String version = IcebergBuild.version();
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("client", "iceberg-java");
    out.put("version", version);
    out.put("build", "iceberg-core, iceberg-data, iceberg-parquet, iceberg-aws, iceberg-aws-bundle " + version
        + ", hadoop-client " + org.apache.hadoop.util.VersionInfo.getVersion() + ", Java " + Runtime.version());
    out.put("checks", checks);
    json.writerWithDefaultPrettyPrinter().writeValue(new File(args[1]), out);
    System.out.println(json.writerWithDefaultPrettyPrinter().writeValueAsString(out));
  }

  /** Runs a check without switches; a storage check that throws runs once more with the s3-region switch. */
  static Map<String, Object> run(RESTCatalog catalog, Check check, boolean storage) {
    try {
      return check.run(catalog);
    } catch (Exception e) {
      if (!storage) {
        return fail(e);
      }
      try {
        if (regionCatalog == null) {
          regionCatalog = Example.connect(Map.of("client.region", plan.at("/region").asText()));
        }
        Map<String, Object> retried = check.run(regionCatalog);
        if ("ok".equals(retried.get("status"))) {
          retried.put("status", "warn");
        }
        retried.put("workaround", "s3-region");
        return retried;
      } catch (Exception ignored) {
        return fail(e);
      }
    }
  }

  static Map<String, Object> catalogLogin(RESTCatalog catalog) {
    Set<String> listed = new HashSet<>();
    for (Namespace namespace : catalog.listNamespaces()) {
      listed.add(namespace.toString());
    }
    List<String> missing = new java.util.ArrayList<>();
    for (String key : List.of("measurement", "latest", "escaped", "view")) {
      String namespace = plan.at("/" + key + "/namespace").asText();
      if (!listed.contains(namespace)) {
        missing.add(namespace);
      }
    }
    Map<String, Object> data = Map.of("namespaces", listed.size());
    return missing.isEmpty()
        ? result("ok", null, data)
        : result("fail", "The namespaces " + missing + " are not listed.", data);
  }

  static Map<String, Object> s3(RESTCatalog catalog) throws IOException {
    Table table = catalog.loadTable(id("measurement", "table"));
    int rows = 0;
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).where(window()).build()) {
      for (Record ignored : records) {
        if (++rows == 5) {
          break;
        }
      }
    }
    return rows > 0
        ? result("ok", null, Map.of("rows", rows))
        : result("inconclusive", "The measurement table has no rows in the window.", Map.of("rows", rows));
  }

  static Map<String, Object> cdcTables(RESTCatalog catalog) throws IOException {
    Table table = catalog.loadTable(id("measurement", "table"));
    Expression device = Expressions.equal("source", plan.at("/measurement/source").asText());
    String boundary = plan.at("/window/boundary").asText();

    Map<String, Object> data = new LinkedHashMap<>();
    data.put("total", count(table, window()));
    data.put("device", count(table, Expressions.and(window(), device)));
    data.put("not_device", count(table, Expressions.and(window(), Expressions.not(device))));
    data.put("before", count(table, Expressions.and(window(), Expressions.lessThan("time", boundary))));
    data.put("after", count(table, Expressions.and(window(), Expressions.greaterThanOrEqual("time", boundary))));

    long total = (long) data.get("total");
    long bySource = (long) data.get("device") + (long) data.get("not_device");
    long byTime = (long) data.get("before") + (long) data.get("after");
    if (bySource != total || byTime != total) {
      return result("fail", "device + not_device = " + bySource + " and before + after = " + byTime
          + ", but total = " + total + ".", data);
    }
    if ((long) data.get("device") == 0 || (long) data.get("before") == 0 || (long) data.get("after") == 0) {
      return result("inconclusive", "The window has no rows for the device or on one side of the boundary.", data);
    }
    return result("ok", null, data);
  }

  /** The latest table has no time column, so it is counted over its current snapshot, in one scan. */
  static Map<String, Object> latestTables(RESTCatalog catalog) throws IOException {
    Table table = catalog.loadTable(id("latest", "table"));
    long rows = 0;
    Set<Object> ids = new HashSet<>();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).select("id").build()) {
      for (Record record : records) {
        rows++;
        ids.add(record.getField("id"));
      }
    }
    Map<String, Object> data = Map.of("rows", rows, "ids", ids.size());
    if (rows == 0) {
      return result("inconclusive", "The latest table has no rows.", data);
    }
    return rows == ids.size()
        ? result("ok", null, data)
        : result("fail", rows + " rows came back for " + ids.size() + " distinct ids.", data);
  }

  static Map<String, Object> identifiers(RESTCatalog catalog, Table table, Record row) throws IOException {
    Table escaped = catalog.loadTable(id("escaped", "table"));
    if (first(escaped, Expressions.equal(plan.at("/escaped/column").asText(), runId())) == null) {
      return missing(escaped, "The escaped table");
    }
    List<String> wrong = new ArrayList<>();
    for (Map.Entry<String, JsonNode> name : plan.at("/fixture/expect/names").properties()) {
      if (row.struct().field(name.getKey()) == null) { // field(name) is case-sensitive
        wrong.add(name.getKey() + " is not found under its exact name");
      } else if (!name.getValue().asText().equals(row.getField(name.getKey()))) {
        wrong.add(name.getKey() + ": expected " + name.getValue().asText() + ", got " + row.getField(name.getKey()));
      }
    }
    return wrong.isEmpty() ? result("ok", null, null) : result("fail", String.join("; ", wrong) + ".", null);
  }

  static Map<String, Object> decimal(RESTCatalog catalog, Table table, Record row) {
    Map<String, Object> data = new LinkedHashMap<>();
    List<String> wrong = new ArrayList<>();
    for (Map.Entry<String, JsonNode> column : plan.at("/fixture/expect/decimals").properties()) {
      Object actual = row.getField(column.getKey());
      data.put(column.getKey(), actual instanceof BigDecimal d ? d.toPlainString() : String.valueOf(actual));
      BigDecimal expected = new BigDecimal(column.getValue().asText());
      if (!(actual instanceof BigDecimal d) || d.compareTo(expected) != 0) {
        wrong.add(column.getKey() + ": expected " + expected.toPlainString() + ", got " + data.get(column.getKey()));
      }
    }
    return wrong.isEmpty() ? result("ok", null, data) : result("fail", String.join("; ", wrong) + ".", data);
  }

  /** Runs with the JVM default time zone set to Asia/Kolkata. */
  static Map<String, Object> timestamps(RESTCatalog catalog, Table table, Record row) {
    String column = plan.at("/fixture/expect/timestamp/column").asText();
    Instant expected = Instant.parse(plan.at("/fixture/expect/timestamp/value").asText());
    Object actual = row.getField(column);
    Map<String, Object> data = Map.of(column, String.valueOf(actual));
    return actual instanceof OffsetDateTime t && t.toInstant().equals(expected)
        ? result("ok", null, data)
        : result("fail", column + ": expected " + expected + ", got " + actual + ".", data);
  }

  static Map<String, Object> nesting(RESTCatalog catalog, Table table, Record row) {
    List<String> wrong = new ArrayList<>();
    String path = plan.at("/fixture/expect/nested/column").asText();
    Object node = row.getField(path);
    for (JsonNode step : plan.at("/fixture/expect/nested/path")) {
      if (!(node instanceof Record struct)) {
        break;
      }
      path += "." + step.asText();
      node = struct.getField(step.asText());
    }
    String leaf = plan.at("/fixture/expect/nested/value").asText();
    if (!leaf.equals(node)) {
      wrong.add(path + ": expected " + leaf + ", got " + node);
    }

    String column = plan.at("/fixture/expect/list/column").asText();
    JsonNode expected = plan.at("/fixture/expect/list/value");
    Object actual = row.getField(column);
    if (!(actual instanceof List<?> list) || list.size() != expected.size()) {
      wrong.add(column + ": expected " + expected.size() + " elements, got " + actual);
    } else {
      for (int i = 0; i < list.size(); i++) {
        Record element = (Record) list.get(i);
        Object position = element.getField("position");
        Object label = element.getField("label");
        BigDecimal want = new BigDecimal(expected.get(i).get("position").asText());
        if (!(position instanceof BigDecimal p && p.compareTo(want) == 0)
            || !expected.get(i).get("label").asText().equals(label)) {
          wrong.add(column + "[" + i + "]: expected " + expected.get(i) + ", got " + element);
        }
      }
    }
    return wrong.isEmpty() ? result("ok", null, null) : result("fail", String.join("; ", wrong) + ".", null);
  }

  static Map<String, Object> largeValues(RESTCatalog catalog, Table table, Record row) throws IOException {
    JsonNode large = plan.at("/fixture/expect/large");
    List<String> wrong = new ArrayList<>();
    Map<String, Object> data = new LinkedHashMap<>();
    if (row.getField(large.get("column").asText()) instanceof String text) {
      int chars = text.codePointCount(0, text.length());
      int bytes = text.getBytes(StandardCharsets.UTF_8).length;
      int c = large.get("char").asText().codePointAt(0);
      data.put("chars", chars);
      data.put("bytes", bytes);
      if (chars != large.get("chars").asInt() || bytes != large.get("bytes").asInt()
          || !text.codePoints().allMatch(cp -> cp == c)) {
        wrong.add(large.get("column").asText() + ": expected " + large.get("chars") + " characters and "
            + large.get("bytes") + " bytes, all " + large.get("char").asText() + ", got " + chars + " characters and "
            + bytes + " bytes" + (text.codePoints().allMatch(cp -> cp == c) ? "" : ", not all of them that character"));
      }
    } else {
      wrong.add(large.get("column").asText() + " is not a string: " + row.getField(large.get("column").asText()));
    }

    JsonNode binned = plan.at("/fixture/expect/binned");
    Table trash = catalog.loadTable(id("fixture/trash", "table"));
    Record rejected = null;
    try (CloseableIterable<Record> records = IcebergGenerics.read(trash)
        .where(Expressions.equal("id", plan.at("/fixture/event").asText())).build()) {
      for (Record record : records) {
        if (String.valueOf(record.getField("path")).endsWith(binned.get("path_suffix").asText())) {
          rejected = record;
          break;
        }
      }
    }
    if (rejected == null) {
      if (trash.currentSnapshot() == null) {
        return result("inconclusive", "The trash table has no snapshot, so no rows could come back.", data);
      }
      wrong.add("the trash table has no row for event " + plan.at("/fixture/event").asText() + " with that path");
    } else if (rejected.getField("data") instanceof ByteBuffer bytes) {
      String decoded;
      try {
        decoded = StandardCharsets.UTF_8.newDecoder().decode(bytes.duplicate()).toString();
      } catch (CharacterCodingException e) {
        decoded = null;
      }
      if (!binned.get("data").asText().equals(decoded)) {
        wrong.add("trash data: expected " + binned.get("data").asText() + ", got "
            + (decoded == null ? "bytes that are not valid UTF-8" : decoded));
      }
    } else {
      wrong.add("trash data is not binary: " + rejected.getField("data"));
    }
    return wrong.isEmpty() ? result("ok", null, data) : result("fail", String.join("; ", wrong) + ".", data);
  }

  static Map<String, Object> schemaEvolution(RESTCatalog catalog, Table table, Record row) throws IOException {
    JsonNode evolution = plan.at("/fixture/expect/evolution");
    String column = evolution.get("column").asText();
    if (row.struct().field(column) == null) {
      return result("fail", "The column " + column + " is not in the fixture table.", null);
    }
    Record baseline = first(table, Expressions.equal("runId", evolution.get("baseline_run").asText()));
    if (baseline == null) {
      return result("inconclusive", "The fixture table has no row of the baseline run "
          + evolution.get("baseline_run").asText() + ".", null);
    }
    List<String> wrong = new ArrayList<>();
    if (!evolution.get("run").asText().equals(row.getField(column))) {
      wrong.add("run row: expected " + evolution.get("run").asText() + ", got " + row.getField(column));
    }
    if (baseline.getField(column) != null) {
      wrong.add("baseline row: expected NULL, got " + baseline.getField(column));
    }
    return wrong.isEmpty() ? result("ok", null, null) : result("fail", String.join("; ", wrong) + ".", null);
  }

  /** The appends from the last snapshot before fixture.posted (exclusive) to the first append after it. */
  static Map<String, Object> incrementalRead(RESTCatalog catalog) throws IOException {
    Table table = catalog.loadTable(id("fixture/table", "table"));
    long posted = OffsetDateTime.parse(plan.at("/fixture/posted").asText()).toInstant().toEpochMilli();
    Snapshot from = null;
    Snapshot to = null;
    for (Snapshot s : table.snapshots()) {
      if (s.timestampMillis() < posted && (from == null || s.timestampMillis() > from.timestampMillis())) {
        from = s;
      } else if (s.timestampMillis() >= posted && DataOperations.APPEND.equals(s.operation())
          && (to == null || s.timestampMillis() < to.timestampMillis())) {
        to = s;
      }
    }
    if (from == null || to == null) {
      return result("inconclusive", "The fixture table has no snapshot before fixture.posted or no append after it.",
          null);
    }
    IncrementalAppendScan scan = table.newIncrementalAppendScan()
        .fromSnapshotExclusive(from.snapshotId()).toSnapshot(to.snapshotId()).select("runId");
    // IcebergGenerics reads only a TableScan, so each planned task is read as generic records of its file format.
    List<Object> runIds = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        try (CloseableIterable<Record> records = FormatModelRegistry.<Record, Object>readBuilder(task.file().format(),
                Record.class, table.io().newInputFile(task.file().location()))
            .project(scan.schema()).split(task.start(), task.length()).build()) {
          for (Record record : records) {
            runIds.add(record.getField("runId"));
          }
        }
      }
    }
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("from", String.valueOf(from.snapshotId()));
    data.put("to", String.valueOf(to.snapshotId()));
    data.put("rows", runIds.size());
    return runIds.equals(List.of(runId()))
        ? result("ok", null, data)
        : result("fail", "Expected the one row with runId " + runId() + ", got " + runIds.size() + " rows with runIds "
            + runIds.subList(0, Math.min(5, runIds.size())) + ".", data);
  }

  static Map<String, Object> views(RESTCatalog catalog) {
    View view = catalog.loadView(id("view", "view"));
    List<String> dialects = view.currentVersion().representations().stream()
        .map(r -> ((SQLViewRepresentation) r).dialect())
        .toList();
    return result("na", "Iceberg Java loads the view metadata (dialects " + String.join(", ", dialects)
        + ") but has no SQL engine to run it; the view spec leaves that to engines (iceberg.apache.org/view-spec).",
        null);
  }

  static TableIdentifier id(String key, String name) {
    return TableIdentifier.of(plan.at("/" + key + "/namespace").asText(), plan.at("/" + key + "/" + name).asText());
  }

  static String runId() {
    return plan.at("/fixture/run_id").asText();
  }

  /** Reads the run's row of the fixture table, then runs the check on it. */
  static Check onRunRow(RowCheck check) {
    return catalog -> {
      Table table = catalog.loadTable(id("fixture/table", "table"));
      Record row = first(table, Expressions.equal("runId", runId()));
      return row == null ? missing(table, "The fixture table") : check.run(catalog, table, row);
    };
  }

  static Record first(Table table, Expression filter) throws IOException {
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).where(filter).build()) {
      Iterator<Record> iterator = records.iterator();
      return iterator.hasNext() ? iterator.next() : null;
    }
  }

  /** Only a table without a snapshot excuses a missing row; one with a snapshot that lacks it is a failure. */
  static Map<String, Object> missing(Table table, String what) {
    return table.currentSnapshot() == null
        ? result("inconclusive", what + " has no snapshot, so no rows could come back.", null)
        : result("fail", what + " has a snapshot, but no row with runId " + runId() + " came back.", null);
  }

  static Expression window() {
    return Expressions.and(
        Expressions.greaterThanOrEqual("time", plan.at("/window/from").asText()),
        Expressions.lessThan("time", plan.at("/window/to").asText()));
  }

  static long count(Table table, Expression filter) throws IOException {
    return count(table, filter, "source", "time");
  }

  static long count(Table table, Expression filter, String... columns) throws IOException {
    long rows = 0;
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).where(filter).select(columns).build()) {
      for (Record ignored : records) {
        rows++;
      }
    }
    return rows;
  }

  static Map<String, Object> result(String status, String note, Map<String, Object> data) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("status", status);
    if (note != null) {
      result.put("note", note);
    }
    if (data != null) {
      result.put("data", data);
    }
    return result;
  }

  // The patterns of harness/redact.py, plus the configured client secret itself.
  static final List<Pattern> SECRETS = List.of(
      Pattern.compile("(?i)(\\b(?:sig|x-amz-security-token|x-amz-signature|x-amz-credential)=)[^&\\s\"'<>]+"),
      Pattern.compile("(?i)(\\b(?:bearer|basic)\\s+)[A-Za-z0-9._~+/=-]{8,}"),
      Pattern.compile("(?i)([\"']?\\b(?:access_token|client_secret|credential|token|s3\\.secret-access-key"
          + "|s3\\.session-token|adls\\.sas-token[\\w.-]*)[\"']?\\s*[:=]\\s*[\"']?)[^\"'\\s,}&]+"));

  static Map<String, Object> fail(Exception e) {
    Throwable root = e;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    String text = e.toString().lines().findFirst().orElse("");
    if (root != e) {
      text += " (cause: " + root.toString().lines().findFirst().orElse("") + ")";
    }
    String credential = System.getenv("CATALOG_CREDENTIAL");
    if (credential != null && credential.contains(":")) {
      text = text.replace(credential.substring(credential.indexOf(':') + 1), "REDACTED");
    }
    for (Pattern secret : SECRETS) {
      text = secret.matcher(text).replaceAll("$1REDACTED");
    }
    return result("fail", text.length() > 300 ? text.substring(0, 300) + "..." : text, null);
  }
}
