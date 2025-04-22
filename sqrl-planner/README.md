# SQRL Planner

The planner parses the SQRL script, creates the computation DAG, optimizes the DAG, and produces the deployment artifacts for all components in the data pipeline.
The DataSQRL planner builds on the Apache FLink parser and planner which in turn uses Apache Calcite. Hence, familiarity with Flink and Calcite is required to understand this codebase. 

The planner has the following stages:
- Parser: Converts the SQRL script to individual statements for the planner. 
- Planner: Converts the statements to SqlNodes (i.e. AST) and RelNodes (i.e. relational trees) using Flink's parser and planner.
- DAG: Builds, optimizes, and assembles the computation DAG that a SQRL script defines.
- Physical Plans: The DAG assembly splits the DAG into sub-DAGs that are executed by the configured engines in the pipeline. Each engine produces a physical plan. Post-processors are executed to optimize or otherwise adjust those physical plans.
- API Generation: Generates a GraphQL API for all the endpoints that are exposed in the DAG. This is skipped if the API is provided by the user.
- API Execution Plan: Based on the (generated) API and the compiled physical plans, the physical plan for the server is generated.
- Deployment Assets: The deployment assets are generated from the optimized physical plans and written to the `build/plan` directory.