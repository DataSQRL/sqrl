# DataSQRL Tags: Definition, Propagation and Validation (V1)

> Requirements by Matthias (created 2026-10-30). Status: Draft.

## 1. Summary

Today, tags such as `@pii` are free text inside doc strings. The compiler does not check them, and nothing guarantees that a derived table carries the tags of the data it was computed from.

This change makes tags a declared, typed vocabulary. The compiler will:

1. Validate every tag against definitions in the package configuration.
2. Propagate tags **from input tables to output tables**, combining them by union and maximum value.
3. Require each public table's doc string to contain the tags it inherits, unless they are explicitly declassified.
4. Check that every table carrying a tag has the columns that tag requires, with compatible types.
5. Write every table's and view's tags into the deployment assets it generates.
6. In cloud compilation, forward selected tags to the systems that tables are materialized into: AWS Lake Formation and Kafka topics.

V1 tracks tags **at the table level only**. Tags may be written on columns, but they count toward the table they belong to. The compiler does not track tags column by column, so relational operators and functions need no special handling: every table a query reads contributes all of its tags.

The compiler **never writes tags into doc strings**. It only checks that they are there.

**What the compiler checks, and nothing more.** The compiler validates exactly four things:
- tag names are defined
- tag values are valid
- tags propagate correctly through the DAG, with a `declassify` hint wherever a tag is dropped or lowered
- tables carrying a tag have the columns that tag requires, with compatible types

It does not judge whether a tag or a declassification is *appropriate*.

Its output feeds the rest of the chain:
1. The compiler writes the tags into the deployment assets.
2. Cloud compilation picks them up and hands them to the operator.
3. The operator attaches them to the deployed resources.
4. The access layer enforces policies based on those resource tags.

The deployment assets are also the input for the agent's tag verifiers and judges (§9).

## 2. Tag definitions

**R1.** Tags are defined in the package configuration under `compiler.tags`, an array of objects:

| Field              | Required | Type             | Meaning |
|--------------------|----------|------------------|---------|
| `name`             | yes      | string           | Tag name. |
| `values`           | no       | array of strings | Allowed values, ordered from **lowest to highest**. |
| `required-columns` | no       | array of objects | Columns every table carrying this tag must have. Each entry has a `name` and a `type`. |

```json
"compiler": {
  "tags": [
    { "name": "classification", "values": ["unclassified", "secret", "top_secret"] },
    { "name": "pii", "values": ["masked", "raw", "sensitive"] },
    { "name": "gdpr" },
    { "name": "jurisdiction",
      "required-columns": [ { "name": "jurisdiction", "type": "STRING" } ] }
  ]
}
```

**R2.** Tag names and values must match `[A-Za-z0-9_-]+`: letters, digits, underscores and dashes only, so no whitespace, `=` or `.`. Names must be unique, and values must be unique within a tag. In `required-columns`, every `type` must be a type the compiler can parse, and column names must be unique within a tag. A violation is a configuration error.

**R3.** Tag types:
- **Boolean tag:** `values` is absent. Its only value is `true`.
- **Ordered tag:** `values` is present. The order of the array is the order used to combine values: `max(secret, top_secret) = top_secret`.

**R4.** If `compiler.tags` is absent, tag processing is disabled and behavior is unchanged from today. This keeps existing projects compiling.

## 3. Tag syntax in doc strings

**R5.** A tag is written `@name` or `@name=value`. It must be preceded by the start of the doc string or by whitespace, so text like `user@example.com` is not parsed as a tag. The name and the value each end at the first character outside `[A-Za-z0-9_-]`.

**R6.** `@name` with no value means `@name=true`. For a boolean tag, `@name` and `@name=true` are both valid. For an ordered tag, a value is required.

**R7.** The compiler reads tags from two places:
- **The table's doc string, in full.** This includes column documentation written inside it, such as a `# Columns:` list.
- **Each column's own doc string**, for example an inline doc comment on a column in `CREATE TABLE`.

In V1, tags from both places count toward the **table's tag set**: one tag set per table.

**R8.** The following are compile errors:
- a tag name not defined in `compiler.tags`
- a value not in the tag's `values`
- an ordered tag without a value
- a boolean tag with a value other than `true`

If the same tag appears more than once in a table's doc strings (table-level or columns), the compiler takes the maximum.

## 4. Propagation

These definitions are computed in the logical plan rewriter at the end of planning:

**R9. Declared tags.** A table's declared tags are the tags in its table doc string combined with the tags in all of its column doc strings, taking the maximum value per tag.

**R10. Input tables.** A derived table's input tables are every table its definition reads from in the logical plan, including tables referenced only in joins, filters or subqueries.

**R11. Inherited tags.** A derived table's inherited tags are the union of the effective tags of all its input tables, taking the maximum value per tag. For boolean tags this means the tag is present if any input has it. Source tables (`CREATE TABLE` or imported) inherit nothing.

**R12. Effective tags.** A table's effective tags are its inherited tags combined with its declared tags, taking the maximum per tag. Declassification (R17) is the exception. A doc string can always raise a tag's level. Only declassification can lower or remove one.

**R13.** Effective tags propagate through hidden tables the same way as through public ones. Hidden tables are not checked for declared tags (R14), but they are checked for required columns (R15).

## 5. Validation

**R14. Declared tags on public tables.** For every public table, the compiler checks that each inherited tag that is not declassified appears in the table's declared tags (R9) with a value **greater than or equal to** the inherited value. A tag on the table doc string or on any column of that table satisfies the check.

Relationship fields, such as `Customer.address := SELECT …`, are not validated. They return rows of their target table, and that table's tags cover them.

**R15. Required columns.** For every table, public or hidden, whose effective tags (R12, R17) include a tag that defines `required-columns`, the compiler checks that each required column:
- is present in the table, with names resolved by the compiler's usual identifier rules
- has a type compatible with the required type

Compatibility is decided by the **call-site type compatibility check**: the column must be acceptable wherever a function parameter of the required type would accept it. A tag that is declassified and not declared has no effective value, so it imposes no required columns.

**R16.** Validation reports every violation in one compile, not just the first. Each error names the table and the tag. A missing-tag error also gives the required value, the value found (if any), and the input table(s) the requirement came from. A required-column error gives the column name, the required type, and the actual type (if the column exists).

## 6. Declassification

**R17.** The `declassify` hint lists tag names without the `@`:

```sql
/*+declassify(pii, classification) */
CustomerCountsByState := SELECT state, COUNT(*) AS n FROM Customer_Profile GROUP BY state;
```

For each listed tag:
- The tag is not validated: no error when it is missing or declared at a lower value.
- The table's effective value for that tag becomes whatever its declared tags contain, at whatever level, or no value if nothing is declared.
- That effective value is what downstream tables inherit.

**R18.** Listing a tag that is not defined is a compile error. Listing a tag that the table does not inherit produces a warning.

**R19.** `declassify` is allowed on hidden tables and has the same effect on propagation there.

## 7. Documentation

**R20.** No change. Tags remain part of the doc-string text and reach generated documentation and API schemas (GraphQL, REST, MCP) through the existing doc-string mechanism.

## 8. Deployment assets and tag forwarding

**R21.** The compiler writes the **effective** tags (R12, R17) of every table and view, hidden or public, into the build assets it generates for the pipeline: the pipeline plan/DAG and the json plan that contains the views for each engine. All tags are listed, not only the ones that will be forwarded. Scanning the deployment assets then shows which tags apply to every deployed object. For a public table that passes validation, the listed tags are exactly its declared tags.

The tags also appear in the pipeline visualization: the DAG output the compiler produces lists the effective tags on every table, hidden or public, so the full tag picture can be read straight from the pipeline graph. Each tag is shown with its value.