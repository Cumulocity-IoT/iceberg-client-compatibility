import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;

/** The checks of clients/CONTRACT.md: Checks <plan.json> <out.json>. */
public class Checks {

  interface Check {
    Map<String, Object> run(RESTCatalog catalog) throws Exception;
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
      checks.put("identifiers", run(catalog, Checks::identifiers, true));
      checks.put("views", run(catalog, Checks::views, false));
    } catch (Exception e) {
      checks.put("catalog-login", fail(e));
      for (String id : List.of("s3", "cdc-tables", "latest-tables", "identifiers", "views")) {
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

  /** The escaped table has no time column either; all its rows are read. */
  static Map<String, Object> identifiers(RESTCatalog catalog) throws IOException {
    Table table = catalog.loadTable(id("escaped", "table"));
    String column = plan.at("/escaped/column").asText();
    if (table.schema().findField(column) == null) { // findField is case-sensitive
      return result("fail", "The column " + column + " is not found under its exact name.", null);
    }
    long rows = count(table, Expressions.alwaysTrue(), column);
    Map<String, Object> data = Map.of("rows", rows);
    if (rows == 0) {
      // Only an empty table excuses zero rows; a table with a snapshot that reads as empty is a failure.
      return table.currentSnapshot() == null
          ? result("inconclusive", "The escaped table has no snapshot, so no rows could come back.", data)
          : result("fail", "The escaped table has a snapshot, but no rows came back.", data);
    }
    return result("ok", null, data);
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
