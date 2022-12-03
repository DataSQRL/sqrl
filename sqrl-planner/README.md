The DataSQRL planner takes the abstract syntax tree (AST) of a SQRL script as input and produces the physical plan of an optimized data pipeline as output.

The planner has the following stages:
- **Transpiler**: Converts the SQRL script AST and produces a logical plan with special SQRL annotations. In the process, it resolves any dependencies using the loaders.
- **Logical Plan Converters**: Converts and optimizes the SQRL-specific logical plan into a vanilla SQL logical plan with some physical execution hints.
- **Query Planner**: Takes the API definition (currently, DataSQRL only supports GraphQL schemas) and produces a set of queries.
- **DAG Planner**: Takes the individual SQL logical plans for each table and the API queries and stitches them into one global DAG that is optimized.
- **Physical Planner**: Takes the Optimized DAG and produces a physical plan for each execution engine in the pipeline.