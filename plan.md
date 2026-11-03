# Implementation Plan: DataSQRL Tags (V1)

Implements `requirements.md` (R1–R21). Requirement numbers are noted per step. Each step compiles and keeps the existing test suite green, so the steps can land as separate commits.

**Out of scope for V1 (per requirements and task scoping):** column-level tag tracking, forwarding tags to Lake Formation or Kafka topics in cloud compilation (§1 item 6), and tags in Kafka/Iceberg deployment assets (only the JDBC plan is extended in step 4).

## Design overview

| Concept | Where it lives |
|---|---|
| Tag definitions (from `compiler.tags`) | New `com.datasqrl.planner.tags.TagDefinitions` (plus `TagDefinition`, `RequiredColumn`) built from `CompilerConfig.getTags()` |
| Tag set of one table | New immutable `com.datasqrl.planner.tags.TableTags`: a reference to `TagDefinitions` plus `Map<String,Integer>` from the normalized tag name to the offset into that tag's configured `values` (`1` for boolean tags), plus the invalid tags found while parsing |
| Declared tags (R9) | `Documented.Documentation.tags()`: `TableTags` |
| Inherited and effective tags (R11, R12, R17) | New `TableTags` fields on `TableAnalysis`, computed by a new `TagAnalyzer` |
| Declassification (R17–R19) | New `DeclassifyHint` in `planner/hint` |
| Validation (R14–R16, R18) | `TagAnalyzer.validate(...)`, called where `TableAnalysis` is built |
| Output (R21) | `PipelineDAGExporter` nodes and `JdbcStatementModel.tags` |

**Disabled mode (R4):** when `compiler.tags` is absent, `TagDefinitions.DISABLED` is used and every tag set is `TableTags.EMPTY`. Then the doc-string parser does not extract tags, the analyzer computes no tags, the `declassify` hint is accepted and ignored, and the outputs have no tags. All tag fields are serialized only when they are non-empty (`@JsonInclude(NON_EMPTY)` / `NON_NULL`), so existing snapshots don't change.

All tag logic sits in one class, `TagAnalyzer`, which both call sites (`SQRLLogicalPlanAnalyzer` and `Sqrl2FlinkSQLTranslator`) delegate to. That keeps the rules in one place.

---

## Step 1: Configuration (R1–R4) ✅ Completed

> Done. Also includes the `ErrorCollector.error(...)` part and the "stop after planning" part of 3.0, because config validation already needs them. `TagDefinitions` is built in the `Sqrl2FlinkSQLTranslator` constructor (new overload that takes the `ErrorCollector`), and compilation stops before planning when `compiler.tags` has errors. `SqrlConfig.hasList` was added because `hasKey` ignores arrays.

### 1.1 JSON schema: `sqrl-planner/src/main/resources/jsonSchema/packageSchema.json`
Add under `compiler.properties`:
```json
"tags": {
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "name":   { "type": "string", "pattern": "^[A-Za-z0-9_-]+$" },
      "values": { "type": "array", "minItems": 1, "uniqueItems": true,
                  "items": { "type": "string", "pattern": "^[A-Za-z0-9_-]+$" } },
      "required-columns": { "type": "array", "items": {
          "type": "object",
          "properties": { "name": { "type": "string" }, "type": { "type": "string" } },
          "required": ["name", "type"], "additionalProperties": false } }
    },
    "required": ["name"],
    "additionalProperties": true
  }
}
```
The schema covers the patterns and the shape. The rules it can't express go in 1.3: unique names (case-insensitive, after lower-casing), unique column names per tag, and parseable types.

### 1.2 Config interfaces and implementation
- `PackageJson.CompilerConfig`: add `Optional<TagDefinitions> getTags()`, or return `TagDefinitions` and use `DISABLED` when `compiler.tags` is absent.
- New `PackageJson.TagConfig` interface (`getName()`, `getValues()`, `getRequiredColumns()`) and `RequiredColumnConfig` (`getName()`, `getType()`), with `TagConfigImpl` / `RequiredColumnConfigImpl` in `com.datasqrl.config`, following the `ExplainConfigImpl` pattern.
- `CompilerConfigImpl.getTags()`: return empty when `!sqrlConfig.hasKey("tags")`. Otherwise read the list of objects (`asList("tags", Map.class)`, or a small Jackson-mapped POJO if `asList` with a record class works with `SqrlConfig`; check that first). The default `package.json` must **not** gain a `tags` entry, because that would enable tags for everyone.
- Check that config merging replaces the `tags` array wholesale (arrays are replaced, per `configuration.md`).

### 1.3 `TagDefinitions` (new, `com.datasqrl.planner.tags`)
- `TagDefinitions.DISABLED`, `isEnabled()`, `Optional<TagDefinition> get(String name)` (lower-case lookup).
- `TagDefinition`: `name`, `List<String> values` (empty means boolean tag), `List<RequiredColumn> requiredColumns`, `boolean isBoolean()`, `OptionalInt offsetOf(String value)` (index into `values`; for boolean tags `1` for `"true"`, empty otherwise), `String valueAt(int offset)` (`"true"` for boolean tags).
- `static TagDefinitions from(List<TagConfig>, ErrorCollector)`: normalizes names and values to lower case and reports configuration errors (R2) for duplicate names or values after normalization, duplicate required-column names, and invalid patterns, as a fallback to the schema.
- Parsing required-column types: use the planner's existing type parser (`RelDataTypeParser` / `relDataTypeParser.parseToRelDataType`, as in `Sqrl2FlinkSQLTranslator.resolveSqrlPassThroughTableFunction`). That needs a type factory, so parse lazily when the `Sqrl2FlinkSQLTranslator` is constructed (it already receives `CompilerConfig`) and report unparseable types there as configuration errors.
### 1.4 `TableTags` (new, `com.datasqrl.planner.tags`)
An immutable value object for one table's tag set. It is self-contained: it knows its definitions, so merging, comparing and rendering need no outside context.
```java
@Value
public class TableTags {
  public static final TableTags EMPTY = new TableTags(TagDefinitions.DISABLED, Map.of(), List.of());

  TagDefinitions definitions;
  /** normalized tag name -> offset into TagDefinition.values() (1 for boolean tags) */
  Map<String,Integer> offsets;
  /** Tags from doc strings that don't resolve to a definition/value; reported by validation (R8) */
  List<InvalidTag> invalidTags;   // record InvalidTag(String text, Reason reason)
                                  // Reason: UNDEFINED_TAG, INVALID_VALUE, MISSING_VALUE, NON_TRUE_BOOLEAN

  public static Builder builder(TagDefinitions defs);  // add(name, valueOrNull) normalizes, resolves the offset, keeps the max for duplicates (R8), or records an InvalidTag
  public TableTags merge(TableTags other);             // union, Math.max per tag (R3, R11, R12)
  public TableTags declassify(Set<String> tags, TableTags declared); // each listed tag gets the declared offset or is removed (R17)
  public OptionalInt get(String tag);
  public boolean isEmpty();
  public Set<String> names();
  public SortedMap<String,String> toValueMap();        // name -> value name ("true" for boolean), for output (R21)
  public String toString();                            // "classification=secret, pii=raw"
}
```
- `merge` and `declassify` require both sides to reference the same `TagDefinitions` instance (precondition check; there is one instance per compile). Merging with `EMPTY` returns the other side.
- `merge` keeps `invalidTags` only from declared tag sets. Inherited and effective sets never carry them: `TagAnalyzer` drops them, so an error is reported once, at the table where it was written.
- Comparing levels for R14 is a plain `int` comparison: `declared.get(tag) >= inherited.get(tag)`.

### 1.5 Tests
- `TagDefinitionsTest` (unit): valid config; duplicate name (also case-only duplicates); duplicate value; invalid characters; bad required-column type; `offsetOf`/`valueAt` for ordered and boolean tags.
- `TableTagsTest` (unit): the builder normalizes case, `@x` resolves to `1` for a boolean tag, duplicates keep the max, an undefined tag/invalid value/ordered tag without a value/boolean with a value other than `true` each become an `InvalidTag`; `merge` is a union with max; `declassify` lowers or removes; `toValueMap()`/`toString()` render value names; mixing definitions is rejected.
- Config/schema test with a `package.json` containing `compiler.tags`: it validates and `getTags()` returns the definitions. An existing config test without tags returns `DISABLED`.

---

## Step 2: Parsing tags from doc strings (R5–R9) ✅ Completed

> Done. Column comments are read from Flink's resolved schema of the created table (`AddTableResult.columnComments`). That schema already contains the columns merged in through `LIKE` and keeps the redefining statement's comment for redefined columns, so decision 3 needs no separate bookkeeping. `HintsAndDoc` carries the `TagDefinitions` and the column tags (`withColumnTags`). The `TableAnalysis` fields from 2.5 come with step 3, where they are filled.

### 2.1 `Documented.Documentation`
Add a component: `record Documentation(String docString, Map<Name,String> columnDocs, Map<Name,String> argumentDocs, TableTags tags)`.
- Update `Documented.EMPTY`, keep a 3-argument convenience constructor (it delegates with `TableTags.EMPTY`) to limit churn, and add `hasTags()`.
- `isEmpty()` must stay as it is (it doesn't consider tags). Otherwise the base-table doc inheritance in `SQRLLogicalPlanAnalyzer` (~L285) changes behavior.

### 2.2 `DocStringParser`
- New overload `parse(String docString, TagDefinitions defs)`. The existing `parse(String)` delegates with `TagDefinitions.DISABLED`, which extracts no tags (R4).
- Extract tags from the **full raw** doc string before columns and arguments are split out, so tags inside `# Columns:` items count toward the table (R7). Pattern:
  `(?<=^|\s)@([A-Za-z0-9_-]+)(?:=([A-Za-z0-9_-]+))?` (R5: start of string or whitespace before `@`, so `user@example.com` isn't matched).
- Feed every match into `TableTags.builder(defs).add(name, value)`. The builder normalizes to lower case, treats a bare `@name` as `true` (R6), resolves the offset, and keeps the max for duplicates (R8). An unresolvable tag becomes an `InvalidTag`, which validation reports in step 3, so the parser itself needs no `ErrorCollector`.
- Leave the tag text in `docString`: tags remain part of the doc text (R20), and the compiler never rewrites doc strings.

### 2.3 Getting `TagDefinitions` into the parser
`HintsAndDoc.getDocumentation()` has many callers. Add a `TagDefinitions tagDefinitions` component to `HintsAndDoc` (default `DISABLED`; update `EMPTY`, `dropHints`, `updateDocsIfAbsent`). Set it in `SqlScriptPlanner.getHintsAndDoc(...)`, where `packageJson.getCompilerConfig()` is already available (see `SqlScriptPlanner` L196). `getDocumentation()` then calls `DocStringParser.parse(doc, tagDefinitions)`.

### 2.4 Column doc strings (R7, second bullet)
For `CREATE TABLE`, also parse each column's own comment (`SqlTableColumn#getComment()`) on the `SqlCreateTable` in `Sqrl2FlinkSQLTranslator.addTable`/`createTable` (~L786–L880). Fold those tags into `Documentation.tags` with `TableTags.merge` (which also carries over their `InvalidTag`s, since these are declared tags) before `addSourceTable` builds the analysis. **Decided:** `CREATE TABLE ... LIKE` inherits column-comment tags from the LIKE source. That source can be another `CREATE TABLE` or a schema file. Doc strings are already inherited through `updateDocumentationFromLike` (`createTableDocumentation` map). Store the source's column-comment `TableTags` in that map, or a sibling map keyed by the same identifier, and `merge` them into the new table's declared tags. Columns the `LIKE` statement redefines use their own comment. Test this in `DocStringParserTest`/the translator and in 5.2.

### 2.5 `TableAnalysis`
`TableAnalysis` already exposes `getDocumentation()`, so declared tags are available as `getDocumentation().tags()`. In step 3, add:
```java
/** Effective tags (R12, R17): what downstream tables inherit and what is written to outputs. */
@Builder.Default TableTags tags = TableTags.EMPTY;
/** Inherited tags (R11), kept for error messages and debugging. */
@Builder.Default @Exclude TableTags inheritedTags = TableTags.EMPTY;
```
Add `default TableTags getTags()` (returning `EMPTY`) to `TableOrFunctionAnalysis`. `SqrlTableFunction` returns its `functionAnalysis.getTags()`, so table functions in `fromTables` propagate too.

### 2.6 Tests
`DocStringParserTest` (extend or create): `@pii`, `@pii=RAW` resolves to the offset of `raw`, the email case, tags inside a `# Columns` item, duplicates take the max, an unknown tag ends up in `invalidTags`, disabled mode returns `TableTags.EMPTY`, column/argument parsing unchanged.

---

## Step 3: Propagation, declassification and validation (R10–R19) ✅ Completed

> Done. Notes on the implementation:
> - The tag settings for one analysis are bundled in a `TagContext` record: definitions, table name for messages, `validateDeclared`, and an optional error collector for tag errors.
> - `validateDeclared` is `!isHidden` from `SqlScriptPlanner` (and `!isRelationship` for functions). `isHidden` also covers test tables outside `sqrl test` and workload tables.
> - The internal scan view of a `CREATE TABLE` keeps its silent `ErrorCollector.root()`, so no unrelated existing warnings change. Only its tag errors go to the statement's collector through `TagContext.errors`. This replaces "pass the statement's collector to `analyzeView`" from 3.0.
> - When a table inherits its base table's documentation, the base table's invalid tags are dropped, so they are reported once.
> - INSERT queries are analyzed without tags (`TagContext.DISABLED`).
> - Verified end to end with a smoke compile of the failing case drafted for 5.2: all violations are reported in one compile, and a table that reads `Customers` only in an `IN` subquery inherits its tags (R10). All 86 `UseCaseCompileTest` snapshots are unchanged.

### 3.0 Non-throwing errors in `ErrorCollector` (R16)
Today `fatal(label, msg, args)` (`sqrl-planner/.../error/ErrorCollector.java` L202–L207) records a `FATAL` message and throws a `CollectedException` when `abortOnFatal` is set, which is the default (`DEFAULT_ABORT_ON_FATAL = true`). `SqlScriptPlanner.planMain` (~L248) rethrows that exception, so a compile currently stops at the first error.
- Add:
  ```java
  /** Records a FATAL error without throwing, so compilation can continue and report further errors. */
  public void error(String msg, Object... args) { error(ErrorLabel.GENERIC, msg, args); }

  public void error(ErrorLabel label, String msg, Object... args) {
    exception(label, msg, args); // records the FATAL message; the returned exception is discarded
  }
  ```
  Recorded errors still fail the compile, because `ErrorCollection.hasErrors()` checks for any `FATAL` message. Factor the message creation out of `exception(...)` into a private `record(...)` method so `error` doesn't build an exception it throws away.
- **Stop after planning when errors were recorded.** `CompilationProcess.executeCompilationInternal` (`sqrl-cli/.../compile/CompilationProcess.java` ~L80) goes from `planner.planMain(...)` straight into `dagPlanner.optimize(...)`. Add `if (errors.hasErrors()) throw new CollectedException(...)` right after `planMain`. `CollectedException` is the exception `fatal` already throws, so the CLI reports it the same way and adding no new message is preferred. Alternatively, return an empty result: `AbstractCompileCmd` (L135) already returns before generating deployment artifacts when `errors.hasErrors()`. Either way, DAG optimization and physical planning don't run on a plan with known errors.
- **Use the real error collector.** `Sqrl2FlinkSQLTranslator.addSourceTable` (~L835) calls `analyzeView(..., ErrorCollector.root())`, a fresh collection, so tag errors on `CREATE TABLE` would be lost. Pass the statement's collector (`lineErrors` from `SqlScriptPlanner`) through `createTable(...)` → `addSourceTable(...)`. Check the other `analyzeView` callers the same way, and also that `TableAnalysis.errors` isn't left at its `ErrorCollector.root()` default on any path that runs tag validation.
- Tests: `ErrorCollectorTest` with `givenCollector_whenError_thenRecordsFatalWithoutThrowing`: `hasErrors()` is true, nothing is thrown, the message is recorded with severity `FATAL`, and it's kept under `onlyErrors()`. The `-fail` use case in 5.2 checks the whole flow end to end: several violations across several tables and statements show up in one compile.

### 3.1 `DeclassifyHint` (new, `planner/hint`)
Model it on `NoQueryHint`: `HINT_NAME = "declassify"`, `Type.ANALYZER`, options = tag names. It needs at least one option. Lower-case the names. Register it through `@AutoService(Factory.class)`. Expose it with `PlannerHints.getHint(DeclassifyHint.class)`.

### 3.2 `TagAnalyzer` (new, `com.datasqrl.planner.tags`)
```java
record TagResult(TableTags inherited, TableTags effective) {}

TagResult compute(List<TableOrFunctionAnalysis> fromTables, TableTags declared,
                  Set<String> declassified);
void validate(String tableName, TagResult result, TableTags declared,
              Set<String> declassified, List<TableOrFunctionAnalysis> fromTables,
              RelDataType rowType, boolean validateDeclared, ErrorCollector errors);
```
- **inherited** = `merge` over `fromTables.getTags()`, starting from an empty set for the compile's `TagDefinitions` (R11). Source tables have no `fromTables` (the root source/sink analysis), so they inherit nothing.
- **effective** = `inherited.merge(declared).declassify(declassified, declared)` (R12, R17), with `invalidTags` dropped (1.4).
- **validate:**
  1. Report each `declared.getInvalidTags()` entry (R8): undefined tag, value not in `values`, ordered tag without a value, boolean tag with a value other than `true`.
  2. Each declassified tag is defined (error) and inherited (warning otherwise) (R18).
  3. If `validateDeclared`: for every inherited tag that isn't declassified, `declared.get(tag)` is present and `>=` the inherited offset (R14). The error names the table, the tag, the required value and the found value (if any), both rendered with `TagDefinition.valueAt`, and the input table(s) whose tags hold that offset (R16).
  4. For every effective tag with `required-columns` (R15, all tables including hidden ones): the column exists (resolve the name with the same `Name.system(...)`/case rules the planner uses for fields), and its type is call-site compatible with the required type. **Decided:** there's no ready-made call-site helper (`TypeCompatibility` only offers `isBackwardsCompatible`), so add `TypeCompatibility.isCallSiteCompatible(actual, required)`, implemented with Flink's `LogicalTypeCasts.supportsImplicitCast(...)` on `FlinkTypeFactory.toLogicalType(...)`. That is how Flink decides whether a function argument is accepted. Unit-test it: `INT` → `BIGINT` ok, `STRING` → `INT` rejected, `VARCHAR(10)` → `STRING` ok, nullability ignored.
- **Report every violation (R16):** report each violation with the new non-throwing `errors.error(...)` (step 3.0), never with `fatal(...)`. Effective tags are computed even when validation fails, so downstream tables keep propagating and get validated in the same compile.

### 3.3 Call site: `SQRLLogicalPlanAnalyzer` (~L293)
- Constructor: add `TagDefinitions tagDefinitions` and `boolean validateDeclaredTags`. Both callers in `Sqrl2FlinkSQLTranslator` (L366, L533) pass `tagDefinitions` (built once in the translator constructor from `compilerConfig`).
- Before `TableAnalysis.builder()`: if enabled, run `compute(sourceTables, documentation.tags(), declassified)` and `validate(viewName, ...)`, then set `.tags(result.effective()).inheritedTags(result.inherited())`.
- `validateDeclared = !Name.isHiddenString(viewName) && validateDeclaredTags` (R13: hidden tables skip R14 but still get R15).
- Note: when a table has no doc string, it inherits its base table's documentation (~L285), and with it the declared tags. A `SELECT *` pass-through with no doc string therefore satisfies R14 automatically. That matches "a doc string can always raise", but call it out in the docs and the banking test.

`analyzeView(...)` gets a `validateDeclaredTags` parameter:
- `true` for table definitions and `addSourceTable` (R14 for public `CREATE TABLE`; inheritance is empty, so only R8/R15 apply in practice).
- `false` from `resolveSqrlTableFunction` when the function is a **relationship** (R14 exclusion). **Decided:** top-level public table functions (`MyFn(x) := SELECT ...`) are validated like tables (`true`). Hidden functions get `false`. `SqlScriptPlanner` knows whether a definition is a relationship and passes that flag through `resolveSqrlTableFunction`. Add a top-level table function with tags to the banking test (5.1).

### 3.4 Call site: `Sqrl2FlinkSQLTranslator.resolveSqrlPassThroughTableFunction` (~L620)
The pass-through function has no analyzable plan, but `fromTables` is given explicitly. Run `TagAnalyzer.compute(fromTables, hintsAndDoc.getDocumentation().tags(), declassified)` (a `TableTags`) and validate it like any other function (same relationship rule as 3.3). Skip R15 here, since there are no real columns beyond the declared return type, or run it against `returnDataType`. Recommendation: run it against `returnDataType`.

Also make `addSqrlTableFunction` (~L670, internal access functions over a base table) copy `baseTable.getTags()` so access functions carry their table's tags without validation.

### 3.5 Input tables include subqueries (R10)
`sourceTables` is filled through `fromSource(...)` (L335/L439/L457), which also covers tables referenced inside `RexSubQuery` (through `subQueryRelShuttle`/`subQueryRexShuttle`, ~L346–L380). Joined tables are covered by the normal visitor. No code change is expected, but **verify it with a test**: a table that reads tag-carrying tables only in `WHERE x IN (SELECT ...)`, only in `EXISTS`, and only as a join filter must inherit their tags. If an input is missing (for example a lateral or correlated table function), add it in the analyzer.

### 3.6 Tests
`TagAnalyzerTest` (unit, with fake `TableOrFunctionAnalysis` inputs): union/max across several inputs, boolean union, raise through a declared tag, declassify to a lower value, declassify to removal, declassify of a tag that isn't inherited (warning), undefined declassify (error), hidden table skips R14 but not R15, missing required column, incompatible type, a tag with `required-columns` that is declassified and not declared imposes no column check (R15), several violations reported together, and for a valid public table `effective == declared` (R21).

---

## Step 4: Output (R21) ✅ Completed

> Done. Tags appear as a `Tags:` header line on each node in `pipeline_explain.txt` and as `tags` in `pipeline_explain.json`/`pipeline_visual.html`. In the JDBC plan, tables read their tags from their engine table's `TableAnalysis`. This also covers `GenericJdbcStatement` tables such as Snowflake's, through `JdbcStatement.getEngineTableTags`. Views get the effective tags of their function: the factory passes them along in the view's `Documentation` (`getViewDocumentation`), so no factory signatures changed. Existing snapshots are unchanged, because tags are omitted when empty.

### 4.1 `PipelineDAGExporter` (`sqrl-planner/.../plan/global/PipelineDAGExporter.java`), used by `DagWriter`
- Add `Map<String,String> tags` (`@JsonInclude(NON_EMPTY)`) to `Node`, filled from `TableTags.toValueMap()` (sorted, value names rather than offsets), and populate it for `Table`, `SourceOrSink` (imports: the source scan view's tags) and `Query` (function tags).
- `Node.toString()`: when non-empty, print a line such as `tags: classification=secret, pii=raw` (each tag with its value; boolean tags show `=true`, consistent with `TableTags.toString()`). That lands in `pipeline_explain.txt`, and through JSON serialization in `pipeline_explain.json` and `pipeline_visual.html`.
- `DagWriter`: tags are always written when present, so no new flag is needed, as long as the exporter populates them in both builder configurations. Optionally show tags in the `visualize_dag.html` node details.

### 4.2 JDBC deployment model
- `sqrl-deployment-model/.../JdbcStatementModel.java`: add a trailing component `Map<String,String> tags` (tag name → value name). It stays a plain map because `sqrl-deployment-model` doesn't depend on `sqrl-planner`, where `TableTags` lives, and deployment consumers need value names, not offsets. The record already uses `@JsonInclude(NON_NULL)`, so use `null` when there are no tags. Update the convenience constructors to pass `null`.
- `JdbcStatement` interface: `@JsonIgnore default Map<String,String> getTags() { return null; }`.
- `CreateTableJdbcStatement` and `GenericJdbcStatement` (views): add a `tags` field. Fill it in `AbstractJdbcStatementFactory` with `TableTags.toValueMap()` (or `null` when empty): for tables from `createTable.tableAnalysis().getTags()` (~L220–L240), for views/queries from `query.function().getFunctionAnalysis().getTags()` (~L138–L162). Use `null` for indexes and extensions.
- `JdbcPhysicalPlan` (L103/L118): pass `statement.getTags()`.
- Update `PlanModelSerializationTest` and `OsProcessManagerTest` to the new constructor arity, and add a round-trip assertion for `tags`.

---

## Step 5: Use-case tests

### 5.1 Banking: positive propagation and declassification
`sqrl-testing/sqrl-testing-integration/src/test/resources/usecases/banking/package.json`: add
```json
"compiler": { "tags": [
  { "name": "pii", "values": ["masked", "raw"] },
  { "name": "classification", "values": ["internal", "confidential"] },
  { "name": "financial" }
] }
```
Leave `package-invalid-operation.json` without tags. It compiles the same `loan.sqrl`, which also covers "the `declassify` hint is ignored when tags are disabled".

`loan.sqrl` changes. Tag the import views; don't touch `banking-shared/sources.sqrl`, which `banking-batch` also uses:
- `/** Customer stream @pii=raw @classification=confidential */ CustomersStream := ...`
- `/** @financial @classification=internal */ ApplicationsStream := ...`
- `Customers`, `Applications`: declare the inherited tags explicitly, or rely on doc inheritance from the base table (cover both: one explicit, one inherited).
- `ApplicationStatus` (joins `ApplicationUpdates`, `Applications`, `LoanTypes`): declares `@financial @classification=confidential` to show a **raise** above the inherited `internal`.
- New hidden table `_ApplicationsByCustomer := SELECT ... FROM Customers JOIN Applications` with no doc string, then a public `/*+declassify(pii) */ /** Loan totals per loan type @financial @classification=confidential */ LoanTotals := SELECT loan_type_id, SUM(amount) ... FROM _ApplicationsByCustomer GROUP BY loan_type_id;`. This shows propagation through a hidden table plus declassification to removal.
- One table that declassifies to a **lower** level, for example `/*+declassify(classification) */ ... @classification=internal`.
- One **hidden** table with `declassify` (R19), for example `/*+declassify(pii) */ _CustomerRegions := SELECT state, COUNT(*) ... FROM Customers GROUP BY state;`, and a public table over it that declares no `pii` and passes validation.
- One table that reads a tagged table **only in a subquery** (R10), for example `HighValueApplications := SELECT * FROM Applications WHERE customer_id IN (SELECT id FROM Customers WHERE ...)`. It must declare the `pii` tag it inherits from `Customers`.
- Test tables (`CustomerApplicationTest`, ...) are public, so they need tags or a `declassify`. Add those (this also tests that test tables are validated).
- Relationships (`Customers.applications`, `Customers.overview`) stay untagged (R14 exclusion).

Re-run with `mvn clean install -P update-snapshots -pl sqrl-testing/sqrl-testing-integration` and check the diff:
- `pipeline_explain.txt` lists effective tags on **every** table node, hidden ones included (R21).
- `postgres.json` (the JDBC plan) has `tags` on tables and views.
- The generated GraphQL schema descriptions still contain the tag text unchanged (R20: no change to doc-string handling, and the compiler never writes tags into doc strings).
- No other snapshot changes.

R21 says "the json plan that contains the views for each engine". All database and query engines build their plans through `AbstractJDBCEngine` → `JdbcPhysicalPlan`, including the Iceberg query engines through `CombinedEnginePlan`, so step 4.2 covers them all. Check this with one Iceberg/DuckDB use case that has tags configured (for example `duckdb`), or assert it in a unit test of `AbstractJdbcStatementFactory`.

### 5.2 Banking: negative cases (`-fail`)
New use case `usecases/banking-tags-fail/` (the `-fail` modifier on the directory snapshots the errors) with a small script over `banking-shared` and tags configured. It covers in one compile: a missing inherited tag, a declared value below the inherited one, an undefined tag, an invalid value, an ordered tag without a value, an undefined `declassify` (error) and a declassify of a tag that isn't inherited (warning), plus a missing or incompatible required column (add `jurisdiction` with `required-columns` to this package only). The snapshot proves R16 (all errors reported, with table, tag, required/found values and source tables).

### 5.3 Tags not analyzed without configuration
Use another, untagged use case: `sensors-doc` (doc-focused, no `compiler.tags`). Add doc strings with `@pii`, `@undefined_tag=xyz` and a derived table that doesn't repeat them, plus a `/*+declassify(pii) */`. It must compile **without errors**, and its snapshot must show no tags. Also add an explicit test to `UseCaseCompileTest`:
```java
@Test
void givenNoTagConfiguration_whenCompile_thenTagsAreNotAnalyzed() throws IOException
```
It compiles `sensors-doc`, asserts success, and asserts that `pipeline_explain.txt`/`.json` and the JDBC plan contain no `tags` entry (AssertJ).

---

## Step 6: Documentation

`documentation/docs/configuration.md`, Compiler section:
- Add `"tags": [...]` to the compiler JSON example with a short comment.
- New subsection `### Tags (compiler.tags)` covering: the definition fields (R1), naming rules (R2), boolean vs. ordered tags (R3), disabled when absent (R4), doc-string syntax with the email example (R5–R7), propagation (union/max, including joins, filters and subqueries, through hidden tables) (R9–R13), validation rules and the relationship exclusion (R14–R16), the `declassify` hint with an example (R17–R19), doc inheritance from the base table, and where tags appear in the outputs (`pipeline_explain.*`, JDBC plan) (R21).

Related updates the CI review guide checks for (commit `3cd250240`): mention the `declassify` hint in the SQRL language hints reference (`documentation/docs/sqrl-language.md`), and update the agent skills under `agent/` that describe doc strings or hints. Optionally regenerate `llms.txt` through the docs build.

---

## Requirements traceability

| Req | Covered by | Verified by |
|---|---|---|
| R1 definitions in `compiler.tags` | 1.1, 1.2 | 1.4 config test, 5.1 |
| R2 naming, uniqueness, parseable types | 1.1 (schema), 1.3 | `TagDefinitionsTest` |
| R3 boolean vs. ordered, order = combine order | 1.3 `TagDefinition.offsetOf`, 1.4 `TableTags.merge` (max of offsets) | `TagDefinitionsTest`, `TableTagsTest` |
| R4 disabled when absent | Design overview, 1.2, 2.2, 3.1 | 5.3 (`sensors-doc` + explicit test), `package-invalid-operation.json` |
| R5 `@name`/`@name=value`, whitespace rule | 2.2 regex | `DocStringParserTest` (email case) |
| R6 bare `@name` = `true`; ordered requires value | 2.2 (normalize), 3.2 validate (1) | `DocStringParserTest`, 5.2 |
| R7 table doc in full + column doc strings | 2.2 (raw string), 2.4 (column comments) | `DocStringParserTest`, 5.1 |
| R8 undefined/invalid errors; duplicates take max | 1.4 builder (`InvalidTag`, max), 3.2 validate (1) | `TableTagsTest`, `DocStringParserTest`, 5.2 |
| R9 declared tags | 1.4, 2.1, 2.2, 2.4 | `DocStringParserTest` |
| R10 input tables incl. joins, filters, subqueries | 3.3 (`sourceTables`), 3.5 | 3.5 test, 5.1 subquery table |
| R11 inherited = union/max; sources inherit nothing | 3.2 compute | `TagAnalyzerTest` |
| R12 effective = inherited ⊕ declared | 3.2 compute | `TagAnalyzerTest`, 5.1 raise case |
| R13 hidden tables propagate; skip R14, keep R15 | 3.3 `validateDeclared` | `TagAnalyzerTest`, 5.1 hidden table |
| R14 public tables declare inherited tags; relationships excluded | 3.2 validate (3), 3.3, 3.4 | 5.1, 5.2 |
| R15 required columns, call-site compatible; declassified and undeclared → none | 3.2 validate (4) | `TagAnalyzerTest`, 5.2 |
| R16 report all violations with details | 3.0 (`ErrorCollector.error`, stop after planning), 3.2 | `ErrorCollectorTest`, 5.2 snapshot |
| R17 `declassify` semantics | 3.1, 3.2 compute | `TagAnalyzerTest`, 5.1 |
| R18 undefined declassify = error; not inherited = warning | 3.2 validate (2) | 5.2 |
| R19 declassify on hidden tables | 3.2 (applies to every table) | 5.1 hidden declassify table |
| R20 doc strings and API docs unchanged | 2.2 (tag text kept) | 5.1 GraphQL snapshot unchanged except new docs |
| R21 effective tags in DAG and engine JSON plans; visible in DAG output; equals declared for valid public tables | 4.1, 4.2 | 5.1 snapshots, `TagAnalyzerTest` |
| §1.6 cloud forwarding (Lake Formation, Kafka) | Out of scope for V1 (see top) | n/a |

## Suggested commit sequence
1. `feat: Add compiler.tags configuration` (step 1, including `TableTags`)
2. `feat: Parse tags from doc strings` (step 2)
3. `feat: Propagate and validate tags with declassify hint` (step 3 + unit tests; 3.0 can go first as its own commit: `feat: Add non-throwing error reporting to ErrorCollector`)
4. `feat: Write tags to pipeline explain and JDBC plan` (step 4)
5. `test: Add tag propagation use cases` (step 5, snapshots)
6. `docs: Document compiler tags` (step 6)

## Resolved decisions
1. Top-level public table functions are validated like tables (R14). Only relationships and hidden functions are excluded (3.3).
2. Call-site type compatibility for R15 uses `LogicalTypeCasts.supportsImplicitCast` through a new `TypeCompatibility.isCallSiteCompatible` (3.2).
3. `CREATE TABLE ... LIKE` inherits column-comment tags from the LIKE source (2.4).
4. R16 (report all violations): `fatal` throws by default and the planner rethrows, so the compile stops at the first error. A new non-throwing `ErrorCollector.error(...)` records violations, and compilation stops after planning when errors were recorded (3.0).
5. Tag sets are `TableTags` (definitions + name → value offset), not `Map<String,String>`. Merging is a max of integers. Output formats render value names through `toValueMap()` (1.4).
