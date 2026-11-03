// Runs the contract checks (clients/CONTRACT.md) with Iceberg Go over the catalog of example.go.
// Usage: iceberg-go-compat <plan.json> <out.json>
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"os"
	"regexp"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
)

type ref struct{ Namespace, Table, Column, View, Source string }

type plan struct {
	Window                    struct{ From, Boundary, To string }
	Measurement, Latest, View ref
	Escaped                   ref
	Fixture                   struct {
		RunID         string `json:"run_id"`
		Event, Posted string
		Table, Trash  ref
		Expect        struct {
			Decimals  map[string]string
			Names     map[string]string
			Timestamp struct{ Column, Value string }
			Nested    struct {
				Column, Value string
				Path          []string
			}
			List struct {
				Column string
				Value  []struct{ Position, Label string }
			}
			Large struct {
				Column, Char string
				Chars, Bytes int
			}
			Binned struct {
				PathSuffix string `json:"path_suffix"`
				Data       string
			}
			Evolution struct {
				Column, Run string
				BaselineRun string `json:"baseline_run"`
			}
		}
	}
}

type result = map[string]any

func status(s, note string, data map[string]any) result {
	r := result{"status": s}
	if note != "" {
		r["note"] = note
	}
	if data != nil {
		r["data"] = data
	}
	return r
}

// verdict is ok without mismatches, else fail with the mismatches as the note.
func verdict(wrong []string, data map[string]any) result {
	if len(wrong) == 0 {
		return status("ok", "", data)
	}
	return status("fail", strings.Join(wrong, "; ")+".", data)
}

type check struct {
	id  string
	run func(context.Context, *rest.Catalog, *plan) (result, error)
}

var checks = []check{
	{"catalog-login", catalogLogin}, {"s3", s3}, {"cdc-tables", cdcTables}, {"latest-tables", latestTables},
	{"identifiers", identifiers}, {"views", views}, {"decimal", decimals}, {"timestamps", timestamps},
	{"nesting", nesting}, {"large-values", largeValues}, {"schema-evolution", schemaEvolution},
	{"incremental-read", incrementalRead},
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: iceberg-go-compat <plan.json> <out.json>")
		os.Exit(2)
	}
	raw, err := os.ReadFile(os.Args[1])
	must(err)
	var p plan
	must(json.Unmarshal(raw, &p))

	ctx := context.Background()
	results := map[string]result{}
	cat, err := connect(ctx)
	for _, c := range checks {
		if err != nil { // the login itself failed, so no check can run
			results[c.id] = failure(err)
			continue
		}
		results[c.id] = safely(ctx, c, cat, &p)
		fmt.Fprintf(os.Stderr, "%s: %s\n", c.id, results[c.id]["status"])
	}
	if cat != nil {
		cat.Close()
	}

	version := strings.TrimPrefix(iceberg.Version(), "v")
	out := map[string]any{
		"client":  "iceberg-go",
		"version": version,
		"build": fmt.Sprintf("iceberg-go %s, arrow-go %s, gocloud.dev %s, Go %s", version,
			module("github.com/apache/arrow-go/v18"), module("gocloud.dev"), strings.TrimPrefix(runtime.Version(), "go")),
		"checks": results,
	}
	text, err := json.MarshalIndent(out, "", "  ")
	must(err)
	must(os.WriteFile(os.Args[2], append(text, '\n'), 0o644))
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, redact(err.Error()))
		os.Exit(1)
	}
}

// safely runs a check, turning an error or a panic (an unexpected Arrow type) into fail.
func safely(ctx context.Context, c check, cat *rest.Catalog, p *plan) (r result) {
	defer func() {
		if e := recover(); e != nil {
			r = failure(fmt.Errorf("panic: %v", e))
		}
	}()
	filterWorkaround = ""
	r, err := c.run(ctx, cat, p)
	if err != nil {
		return failure(err)
	}
	if filterWorkaround != "" && r["status"] == "ok" {
		r["status"], r["workaround"], r["note"] = "warn", "filter-after-scan", filterWorkaround
	}
	return r
}

func module(path string) string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, d := range info.Deps {
			if d.Path == path {
				return strings.TrimPrefix(d.Version, "v")
			}
		}
	}
	return "unknown"
}

// ---- reading

func load(ctx context.Context, cat *rest.Catalog, r ref) (*table.Table, error) {
	return cat.LoadTable(ctx, catalog.ToIdentifier(r.Namespace, r.Table))
}

// batches scans a table and returns the non-empty record batches.
func batches(ctx context.Context, t *table.Table, opts ...table.ScanOption) ([]arrow.RecordBatch, error) {
	_, records, err := t.Scan(opts...).ToArrowRecords(ctx)
	if err != nil {
		return nil, err
	}
	return collect(records)
}

func collect(records func(func(arrow.RecordBatch, error) bool)) ([]arrow.RecordBatch, error) {
	var out []arrow.RecordBatch
	for rec, err := range records {
		if err != nil {
			return nil, err
		}
		if rec.NumRows() > 0 {
			out = append(out, rec)
		}
	}
	return out, nil
}

func count(recs []arrow.RecordBatch) (n int64) {
	for _, r := range recs {
		n += r.NumRows()
	}
	return n
}

func eq(column, value string) iceberg.BooleanExpression {
	return iceberg.EqualTo(iceberg.Reference(column), value)
}

// filterWorkaround says, while a check runs, why its run row needed the filter-after-scan switch; safely reports it.
var filterWorkaround string

// runRow is the row of r with runId = run, projected to fields (dotted names select nested fields). It reads with a
// row filter first, and only when that fails or finds no row, with the filter-after-scan switch.
func runRow(ctx context.Context, cat *rest.Catalog, r ref, run string, fields ...string) (arrow.RecordBatch, error) {
	t, err := load(ctx, cat, r)
	if err != nil {
		return nil, err
	}
	selected := table.WithSelectedFields(append(fields, "runId")...)
	recs, err := batches(ctx, t, table.WithRowFilter(eq("runId", run)), selected)
	if rec := findRun(recs, run); err == nil && rec != nil {
		return rec, nil
	}
	plain := "no row"
	if err != nil {
		plain = fmt.Sprintf("the error %q", redact(err.Error()))
	}

	// Workaround filter-after-scan: iceberg-go 0.7.0 keeps an unselected struct child in the pruned file schema it
	// binds the row filter to (pruneParquetSchema.Struct in table/internal), so with a projection that leaves out a
	// list of structs ahead of the filtered column, the filter is applied to the next column. Planning with the
	// filter still prunes files; the files are then read without it and the rows filtered here.
	tasks, err := t.Scan(table.WithRowFilter(eq("runId", run)), selected).PlanFiles(ctx)
	if err != nil {
		return nil, err
	}
	for i := range tasks {
		tasks[i].Residual = nil // a residual would apply the row filter again
	}
	_, records, err := t.Scan(selected).ReadTasks(ctx, tasks)
	if err != nil {
		return nil, err
	}
	if recs, err = collect(records); err != nil {
		return nil, err
	}
	if rec := findRun(recs, run); rec != nil {
		filterWorkaround = "a row filter combined with a projection that leaves out a list-of-structs column ahead of " +
			"the filtered column is applied to the next column (pruneParquetSchema.Struct in table/internal), here " +
			"giving " + plain + "; the workaround plans the files with the filter, reads them without it and filters " +
			"the rows after the scan. A fix is sketched in https://github.com/Cumulocity-IoT/iceberg-go-fixes/pull/1."
		return rec, nil
	}
	return nil, fmt.Errorf("%s.%s has no row with runId = %s", r.Namespace, r.Table, run)
}

// findRun is the row with runId = run as a one-row batch, nil if there is none.
func findRun(recs []arrow.RecordBatch, run string) arrow.RecordBatch {
	for _, rec := range recs {
		for i := range rec.NumRows() {
			if v, _ := str(column(rec, "runId"), int(i)); v == run {
				return rec.NewSlice(i, i+1)
			}
		}
	}
	return nil
}

func column(rec arrow.RecordBatch, name string) arrow.Array {
	if i := rec.Schema().FieldIndices(name); len(i) > 0 {
		return rec.Column(i[0])
	}
	return nil
}

func field(a arrow.Array, name string) arrow.Array {
	s, ok := a.(*array.Struct)
	if !ok {
		return nil
	}
	if i, ok := s.DataType().(*arrow.StructType).FieldIdx(name); ok {
		return s.Field(i)
	}
	return nil
}

func str(a arrow.Array, i int) (string, bool) {
	s, ok := a.(array.StringLike)
	if !ok || s.IsNull(i) {
		return "", false
	}
	return s.Value(i), true
}

// exact is a decimal value as an exact rational: unscaled value / 10^scale.
func exact(a arrow.Array, i int) (*big.Rat, bool) {
	d, ok := a.(*array.Decimal128)
	if !ok || d.IsNull(i) {
		return nil, false
	}
	scale := big.NewInt(0).Exp(big.NewInt(10), big.NewInt(int64(d.DataType().(*arrow.Decimal128Type).Scale)), nil)
	return new(big.Rat).SetFrac(d.Value(i).BigInt(), scale), true
}

func decimalIs(a arrow.Array, i int, expected string) bool {
	want, ok := new(big.Rat).SetString(expected)
	got, isDecimal := exact(a, i)
	return ok && isDecimal && got.Cmp(want) == 0
}

// shown is a value with its Arrow type, for a note.
func shown(a arrow.Array, i int) string {
	if a == nil {
		return "no such column"
	}
	if a.IsNull(i) {
		return "NULL (" + a.DataType().String() + ")"
	}
	v := a.ValueStr(i)
	if len(v) > 80 {
		v = v[:80] + "..."
	}
	return v + " (" + a.DataType().String() + ")"
}

// ---- checks without fixtures

func catalogLogin(ctx context.Context, cat *rest.Catalog, p *plan) (result, error) {
	namespaces, err := cat.ListNamespaces(ctx, nil)
	if err != nil {
		return nil, err
	}
	var listed, missing []string
	for _, ns := range namespaces {
		listed = append(listed, strings.Join(ns, "."))
	}
	for _, r := range []ref{p.Measurement, p.Latest, p.Escaped, p.View} {
		if !slices.Contains(listed, r.Namespace) && !slices.Contains(missing, r.Namespace) {
			missing = append(missing, r.Namespace)
		}
	}
	data := map[string]any{"namespaces": len(listed)}
	if len(missing) > 0 {
		return status("fail", "ListNamespaces does not list "+strings.Join(missing, ", ")+".", data), nil
	}
	return status("ok", "", data), nil
}

func s3(ctx context.Context, cat *rest.Catalog, p *plan) (result, error) {
	rows, err := readAFewRows(ctx, cat, p.Measurement.Namespace, p.Measurement.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Release()
	if rows.NumRows() == 0 {
		return status("fail", "The scan of the measurement table returned no rows.", nil), nil
	}
	return status("ok", "", map[string]any{"rows": rows.NumRows()}), nil
}

func cdcTables(ctx context.Context, cat *rest.Catalog, p *plan) (result, error) {
	t, err := load(ctx, cat, ref{Namespace: p.Measurement.Namespace, Table: p.Measurement.Table})
	if err != nil {
		return nil, err
	}
	at := iceberg.Reference("time")
	start, end := iceberg.GreaterThanEqual(at, p.Window.From), iceberg.LessThan(at, p.Window.To)
	device := eq("source", p.Measurement.Source)
	filters := map[string]iceberg.BooleanExpression{
		"total":      iceberg.NewAnd(start, end),
		"device":     iceberg.NewAnd(start, end, device),
		"not_device": iceberg.NewAnd(start, end, iceberg.NewNot(device)),
		"before":     iceberg.NewAnd(start, iceberg.LessThan(at, p.Window.Boundary)),
		"after":      iceberg.NewAnd(iceberg.GreaterThanEqual(at, p.Window.Boundary), end),
	}
	n := map[string]int64{}
	for name, filter := range filters {
		recs, err := batches(ctx, t, table.WithRowFilter(filter), table.WithSelectedFields("source"))
		if err != nil {
			return nil, err
		}
		n[name] = count(recs)
	}
	data := map[string]any{}
	for k, v := range n {
		data[k] = v
	}
	if n["device"]+n["not_device"] != n["total"] || n["before"]+n["after"] != n["total"] {
		return status("fail", "The filtered counts do not add up to the total.", data), nil
	}
	if min(n["device"], n["before"], n["after"]) == 0 {
		return status("inconclusive", "The window has no rows for the device or on one side of the boundary.", data), nil
	}
	return status("ok", "", data), nil
}

func latestTables(ctx context.Context, cat *rest.Catalog, p *plan) (result, error) {
	t, err := load(ctx, cat, p.Latest)
	if err != nil {
		return nil, err
	}
	recs, err := batches(ctx, t, table.WithSelectedFields("id"))
	if err != nil {
		return nil, err
	}
	ids := map[string]bool{}
	for _, rec := range recs {
		col := column(rec, "id")
		for i := range col.Len() {
			ids[col.ValueStr(i)] = true
		}
	}
	data := map[string]any{"rows": count(recs), "ids": len(ids)}
	if count(recs) == 0 {
		return status("inconclusive", "The latest table returned no rows.", data), nil
	}
	if count(recs) != int64(len(ids)) {
		return status("fail", fmt.Sprintf("%d rows came back for %d distinct ids, so rows replaced by upserts are still returned.",
			count(recs), len(ids)), data), nil
	}
	return status("ok", "", data), nil
}

func views(ctx context.Context, cat *rest.Catalog, p *plan) (result, error) {
	v, err := cat.LoadView(ctx, catalog.ToIdentifier(p.View.Namespace, p.View.View))
	if err != nil {
		return nil, err
	}
	var dialects []string
	for _, r := range v.CurrentVersion().Representations {
		dialects = append(dialects, r.Dialect)
	}
	return status("na", "Iceberg Go loads the view metadata (dialects "+strings.Join(dialects, ", ")+
		") but has no SQL engine to run it; the view spec leaves that to engines (iceberg.apache.org/view-spec).", nil), nil
}

// ---- checks on the fixture data

func identifiers(ctx context.Context, cat *rest.Catalog, p *plan) (result, error) {
	if _, err := runRow(ctx, cat, p.Escaped, p.Fixture.RunID, p.Escaped.Column); err != nil {
		return nil, err
	}
	names := p.Fixture.Expect.Names
	keys := slices.Sorted(maps.Keys(names))
	t, err := load(ctx, cat, p.Fixture.Table)
	if err != nil {
		return nil, err
	}
	var wrong []string
	for _, k := range keys {
		if !slices.ContainsFunc(t.Schema().Fields(), func(f iceberg.NestedField) bool { return f.Name == k }) {
			wrong = append(wrong, "the schema has no column named exactly "+k)
		}
	}
	if len(wrong) > 0 {
		return verdict(wrong, nil), nil
	}
	rec, err := runRow(ctx, cat, p.Fixture.Table, p.Fixture.RunID, keys...)
	if err != nil {
		return nil, err
	}
	for _, k := range keys {
		if v, ok := str(column(rec, k), 0); !ok || v != names[k] {
			wrong = append(wrong, fmt.Sprintf("%s: expected %q, got %s", k, names[k], shown(column(rec, k), 0)))
		}
	}
	return verdict(wrong, nil), nil
}

func decimals(ctx context.Context, cat *rest.Catalog, p *plan) (result, error) {
	expected := p.Fixture.Expect.Decimals
	keys := slices.Sorted(maps.Keys(expected))
	rec, err := runRow(ctx, cat, p.Fixture.Table, p.Fixture.RunID, keys...)
	if err != nil {
		return nil, err
	}
	var wrong []string
	for _, k := range keys {
		if !decimalIs(column(rec, k), 0, expected[k]) {
			wrong = append(wrong, fmt.Sprintf("%s: expected %s, got %s", k, expected[k], shown(column(rec, k), 0)))
		}
	}
	return verdict(wrong, nil), nil
}

// timestamps reads with the process time zone (time.Local) set to Asia/Kolkata.
func timestamps(ctx context.Context, cat *rest.Catalog, p *plan) (result, error) {
	kolkata, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		return nil, err
	}
	local := time.Local
	time.Local = kolkata
	defer func() { time.Local = local }()

	t := p.Fixture.Expect.Timestamp
	expected, err := time.Parse(time.RFC3339Nano, t.Value)
	if err != nil {
		return nil, err
	}
	rec, err := runRow(ctx, cat, p.Fixture.Table, p.Fixture.RunID, t.Column)
	if err != nil {
		return nil, err
	}
	col := column(rec, t.Column)
	ts, ok := col.(*array.Timestamp)
	typ, _ := col.DataType().(*arrow.TimestampType)
	if !ok || typ.TimeZone == "" || ts.IsNull(0) {
		return status("fail", fmt.Sprintf("%s: expected the instant %s, got %s.", t.Column, t.Value, shown(col, 0)), nil), nil
	}
	toTime, err := typ.GetToTimeFunc()
	if err != nil {
		return nil, err
	}
	actual := toTime(ts.Value(0))
	data := map[string]any{t.Column: actual.In(time.Local).Format(time.RFC3339Nano)}
	if !actual.Equal(expected) {
		return status("fail", fmt.Sprintf("%s: expected the instant %s, got %s.", t.Column, t.Value, data[t.Column]), data), nil
	}
	return status("ok", "", data), nil
}

func nesting(ctx context.Context, cat *rest.Catalog, p *plan) (result, error) {
	n, l := p.Fixture.Expect.Nested, p.Fixture.Expect.List
	leafPath := strings.Join(append([]string{n.Column}, n.Path...), ".")
	rec, err := runRow(ctx, cat, p.Fixture.Table, p.Fixture.RunID, leafPath, l.Column)
	if err != nil {
		return nil, err
	}
	var wrong []string
	leaf := column(rec, n.Column)
	for _, key := range n.Path {
		if leaf = field(leaf, key); leaf == nil {
			break
		}
	}
	if v, ok := str(leaf, 0); leaf == nil || !ok || v != n.Value {
		wrong = append(wrong, fmt.Sprintf("%s: expected %q, got %s", leafPath, n.Value, shown(leaf, 0)))
	}

	list, ok := column(rec, l.Column).(array.ListLike)
	if !ok {
		return verdict(append(wrong, l.Column+" is not a list: "+shown(column(rec, l.Column), 0)), nil), nil
	}
	start, end := list.ValueOffsets(0)
	items := list.ListValues()
	if int(end-start) != len(l.Value) {
		wrong = append(wrong, fmt.Sprintf("%s: expected %d elements, got %d", l.Column, len(l.Value), end-start))
	}
	for k, want := range l.Value {
		i := int(start) + k
		if i >= int(end) {
			break
		}
		label, _ := str(field(items, "label"), i)
		if !decimalIs(field(items, "position"), i, want.Position) || label != want.Label {
			wrong = append(wrong, fmt.Sprintf("%s[%d]: expected {%s %s}, got {%s %s}", l.Column, k, want.Position, want.Label,
				shown(field(items, "position"), i), shown(field(items, "label"), i)))
		}
	}
	return verdict(wrong, nil), nil
}

func largeValues(ctx context.Context, cat *rest.Catalog, p *plan) (result, error) {
	large, binned := p.Fixture.Expect.Large, p.Fixture.Expect.Binned
	rec, err := runRow(ctx, cat, p.Fixture.Table, p.Fixture.RunID, large.Column)
	if err != nil {
		return nil, err
	}
	var wrong []string
	text, ok := str(column(rec, large.Column), 0)
	chars, bytes := utf8.RuneCountInString(text), len(text)
	data := map[string]any{"chars": chars, "bytes": bytes}
	same := strings.Trim(text, large.Char) == ""
	if !ok || chars != large.Chars || bytes != large.Bytes || !same {
		wrong = append(wrong, fmt.Sprintf("%s: expected %d characters and %d bytes of %q, got %d characters and %d bytes (all of them that character: %t)",
			large.Column, large.Chars, large.Bytes, large.Char, chars, bytes, same))
	}

	trash, err := load(ctx, cat, p.Fixture.Trash)
	if err != nil {
		return nil, err
	}
	recs, err := batches(ctx, trash, table.WithRowFilter(eq("id", p.Fixture.Event)), table.WithSelectedFields("path", "data"))
	if err != nil {
		return nil, err
	}
	var blob arrow.Array
	at := 0
	for _, r := range recs {
		for i := range int(r.NumRows()) {
			if path, _ := str(column(r, "path"), i); blob == nil && strings.HasSuffix(path, binned.PathSuffix) {
				blob, at = column(r, "data"), i
			}
		}
	}
	switch b, isBinary := blob.(interface{ Value(int) []byte }); {
	case blob == nil:
		wrong = append(wrong, fmt.Sprintf("the trash table has no row with id = %s and a path ending in %s", p.Fixture.Event, binned.PathSuffix))
	case !isBinary || blob.IsNull(at):
		wrong = append(wrong, "the binned data is not binary: "+shown(blob, at))
	case !utf8.Valid(b.Value(at)) || string(b.Value(at)) != binned.Data:
		wrong = append(wrong, fmt.Sprintf("binned data: expected %q, got %q", binned.Data, b.Value(at)))
	}
	return verdict(wrong, data), nil
}

func schemaEvolution(ctx context.Context, cat *rest.Catalog, p *plan) (result, error) {
	e := p.Fixture.Expect.Evolution
	run, err := runRow(ctx, cat, p.Fixture.Table, p.Fixture.RunID, e.Column)
	if err != nil {
		return nil, err
	}
	baseline, err := runRow(ctx, cat, p.Fixture.Table, e.BaselineRun, e.Column)
	if err != nil {
		return nil, err
	}
	var wrong []string
	if v, ok := str(column(run, e.Column), 0); !ok || v != e.Run {
		wrong = append(wrong, fmt.Sprintf("run row: expected %q, got %s", e.Run, shown(column(run, e.Column), 0)))
	}
	if b := column(baseline, e.Column); b == nil || !b.IsNull(0) {
		wrong = append(wrong, "baseline row: expected NULL, got "+shown(b, 0))
	}
	return verdict(wrong, nil), nil
}

// incrementalRead reads the files appended between the last snapshot before the fixture was posted
// and the first append snapshot after it, with the incremental append scan.
func incrementalRead(ctx context.Context, cat *rest.Catalog, p *plan) (result, error) {
	t, err := load(ctx, cat, p.Fixture.Table)
	if err != nil {
		return nil, err
	}
	posted, err := time.Parse(time.RFC3339Nano, p.Fixture.Posted)
	if err != nil {
		return nil, err
	}
	current := t.CurrentSnapshot()
	if current == nil {
		return status("inconclusive", "The fixture table has no snapshot.", nil), nil
	}
	var from, to *table.Snapshot
	for _, s := range table.AncestorsOf(current.SnapshotID, t.Metadata().SnapshotByID) { // newest first
		if time.UnixMilli(s.TimestampMs).Before(posted) {
			from = &s
			break
		}
		if s.Summary != nil && s.Summary.Operation == table.OpAppend {
			to = &s
		}
	}
	if to == nil {
		return status("inconclusive", "The fixture table has no append snapshot after the fixture was posted.", nil), nil
	}
	scan := t.NewIncrementalAppendScan().ToSnapshot(to.SnapshotID)
	data := map[string]any{"to": to.SnapshotID, "added_records": to.Summary.Properties["added-records"]}
	if from != nil {
		scan = scan.FromSnapshotExclusive(from.SnapshotID)
		data["from"] = from.SnapshotID
	}
	tasks, err := scan.PlanFiles(ctx)
	if err != nil {
		return nil, err
	}
	// The planned tasks carry no projection; they are read with a scan of the same snapshot.
	_, records, err := t.Scan(table.WithSnapshotID(to.SnapshotID), table.WithSelectedFields("runId")).ReadTasks(ctx, tasks)
	if err != nil {
		return nil, err
	}
	recs, err := collect(records)
	if err != nil {
		return nil, err
	}
	var runs []string
	for _, rec := range recs {
		for i := range int(rec.NumRows()) {
			v, _ := str(column(rec, "runId"), i)
			runs = append(runs, v)
		}
	}
	data["rows"] = len(runs)
	if len(runs) != 1 || runs[0] != p.Fixture.RunID {
		if len(runs) > 5 {
			runs = append(runs[:5], "...")
		}
		return status("fail", fmt.Sprintf("Expected only the run's row %s, got %d rows with runId %v.", p.Fixture.RunID, data["rows"], runs), data), nil
	}
	return status("ok", "", data), nil
}

// ---- secrets

// The patterns of harness/redact.py: each keeps group 1 and masks what follows.
var secrets = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(\b(?:sig|x-amz-security-token|x-amz-signature|x-amz-credential)=)[^&\s"'<>]+`),
	regexp.MustCompile(`()\b(?:AKIA|ASIA)[0-9A-Z]{16}\b`),
	regexp.MustCompile(`(?i)(\b(?:bearer|basic)\s+)[A-Za-z0-9._~+/=-]{8,}`),
	regexp.MustCompile(`(?i)(["']?\b(?:access_token|client_secret|credential|token|s3\.access-key-id|s3\.secret-access-key` +
		`|s3\.session-token|adls\.sas-token[\w.-]*)["']?\s*[:=]\s*["']?)[^"'\s,}&]+`),
}

func redact(text string) string {
	if _, secret, ok := strings.Cut(os.Getenv("CATALOG_CREDENTIAL"), ":"); ok && len(secret) >= 4 {
		text = strings.ReplaceAll(text, secret, "REDACTED")
	}
	for _, re := range secrets {
		text = re.ReplaceAllString(text, "${1}REDACTED")
	}
	return text
}

func failure(err error) result {
	text, _, _ := strings.Cut(redact(err.Error()), "\n")
	if len(text) > 300 {
		text = text[:300] + "..."
	}
	return status("fail", text, nil)
}
