# Postgres Function Translation

Analysis is done based on the [FlinkSQL Function structured documentation](https://github.com/apache/flink/blob/master/docs/data/sql_functions.yml)
with this prompt:
```text
Below is a list of FlinkSQL functions with their signature. Identify all functions that are not identical in PostgreSQL 18+ (i.e. they don't have the same name or they don't have the same signature) and describe how that function can be mapped to function calls (either a single function or a function chain) in PostgreSQL. Produce the output as a markdown table with two columns: The signature of the FlinkSQL function that is not identical in PostgreSQL and the mapping. Produce markdown code output.
```

For implementation, we use this prompt in Claude Code:
```text
@sqrl-planner/src/main/java/com/datasqrl/function/translation/postgres/builtinflink/ You are an expert java developer implementing translation classes for built-in FlinkSQL functions in this package. You are given the
  signature of a FlinkSQL function and you implement a translation for that function to PostgreSQL as a class that extends @sqrl-planner/src/main/java/com/datasqrl/function/translation/PostgresSqlTranslation.java. The translation must use existing PostgreSQL functions and produce the same output as the original FlinkSQL function. Lookup the proposed translation in @sqrl-planner/src/main/java/com/datasqrl/function/translation/postgres/builtinflink/README.md to implement. Favor simple translations and use comments sparingly.
  See @sqrl-planner/src/main/java/com/datasqrl/function/translation/postgres/builtinflink/ArrayContainsSqlTranslation.java as an example. 
  Note, that Flink's MAP type is translated to JSONB in PostgreSQL which means functions that operate on maps need to be translated to their corresponding jsonb functions.
  In rare cases, simple translations are insufficient and the translation requires
  rewriting the operator in the relational algebra tree. Only if absolutely necessary do you implement a @sqrl-planner/src/main/java/com/datasqrl/calcite/function/OperatorRuleTransform.java to do so, such as the example
  @sqrl-planner/src/main/java/com/datasqrl/function/translation/postgres/text/TextSearchTranslation.java. 
  Translate all FlinkSQL functions in this test case that do not have a translation already:
  @sqrl-testing/sqrl-testing-integration/src/test/resources/usecases/function-translation/postgres/pg-translation.sqrl
  Once you have completed the translation implementations, run this individual test case to validate:
  @sqrl-testing/sqrl-testing-integration/src/test/java/com/datasqrl/FullUseCaseIT.java#L63-64
  Fix compile errors by updating the translations.
  Ensure that the snapshots produced in the folder @sqrl-testing/sqrl-testing-integration/src/test/resources/usecases/function-translation/postgres/snapshots are correct and identical to @sqrl-testing/sqrl-testing-integration/src/test/resources/usecases/function-translation/flink/snapshots/
```


