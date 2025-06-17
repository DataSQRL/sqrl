# SQRL Testing

Contains primarily integration tests for the planner, compiler, and the entire generated data pipelines.

This is where the bulk of the test coverage for end-to-end DataSQRL pipelines lives.

Important Test Classes:
* DagPlannerTest: Compiles (but does not run) the SQRL scripts under `test/resources/dagplanner`. Use this to test parser and planner features confined to a single SQRL script.
* UseCaseCompileTest: Compiles entire SQRL projects in individual folders under `test/resources/usecases`. Use this to test parser and planner features that require an entire project setup.
* FullUsecasesIT: Runs and Tests entire SQRL projects by standing up the pipeline and executing the tests for projects defined in `test/resources/usecases`. Use this to test runtime components and features.
* GraphQLValidationTest: Validates GraphQL parsing and mapping of GraphQL schemas to SQRL scripts.