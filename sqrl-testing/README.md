# SQRL Testing

Contains primarily integration tests for the planner, compiler, and the entire generated data pipelines.

This is where the bulk of the test coverage for end-to-end DataSQRL pipelines lives.

Important Test Classes:
* DagPlannerTest: Compiles (but does not run) the SQRL scripts under `test/resources/dagplanner`. Use this to test parser and planner features confined to a single SQRL script.
* UseCaseCompileTest: Compiles entire SQRL projects in individual folders under `test/resources/usecases`. Use this to test parser and planner features that require an entire project setup.
* FullUseCaseIT: Runs and Tests entire SQRL projects by standing up the pipeline and executing the tests for projects defined in `test/resources/usecases`. Use this to test runtime components and features.
* GraphQLValidationTest: Validates GraphQL parsing and mapping of GraphQL schemas to SQRL scripts.

## Integration test sharding

CI splits the use-case suite across several shards with
[shard4j](https://github.com/velo/shard4j). A coordinator hands each shard work one
class-batch at a time, slowest-first by durations measured on earlier runs, and answers one
question at the end: did every registered test reach a terminal non-failing state? That
session verdict is the gate, not the shards' exit codes -- a test that fails on one shard
and is retried onto another is a flake, and only the coordinator can tell the difference.

It is off unless you ask for it. `mvn verify` runs the whole suite in one JVM exactly as
before; the `coordinated` profile is what turns it on, and CI is the only thing that
activates it. There is no local coordinator to run and nothing to configure.

Three rules are worth knowing before touching the suite:

- **The `coordinated` profile stays scoped to `sqrl-testing-integration`.** Every JVM that
  registers into one session must discover the same set of tests, so a second module
  registering is rejected outright. Other modules' integration tests run unsharded in the
  `other-integration-tests` job, which is gating in the ordinary way -- the shard jobs
  swallow their exit code, so anything run beside them there would fail silently.
- **Do not skip a use case with a JUnit assumption.** An aborted row makes the whole
  parameterized method report as ABORTED. shard4j 0.5.0 records durations from that
  correctly, but earlier versions read it as "this taught me nothing", never learned the
  per-use-case durations, and handed the entire method to one shard. Prefer excluding the
  case from the parameter set.
- **Deleting a use case reddens one run.** The coordinator hands out invocation positions
  from the durations it recorded last time; a position that no longer exists is reported
  back as vanished, the run fails naming it, and the next run is computed from the
  corrected plan. This is expected, not a bug to chase.

shard4j requires a JUnit Platform matching the one it was built against: 0.5.0 targets
Platform 6, which is what this repository uses.
