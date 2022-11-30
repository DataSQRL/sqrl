package ai.datasqrl.physical;

import ai.datasqrl.physical.pipeline.ExecutionStage;
import ai.datasqrl.physical.stream.flink.plan.FlinkStreamPhysicalPlan;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;

import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@Slf4j
public class PhysicalPlanExecutor {

  public Result execute(PhysicalPlan physicalPlan) {
    List<StageResult> results = new ArrayList<>();
    //Need to execute plans backwards so all subsequent stages are ready before stage executes
    for (PhysicalPlan.StagePlan stagePlan : Lists.reverse(physicalPlan.getStagePlans())) {
      results.add(new StageResult(stagePlan.getStage(), stagePlan.getStage().execute(stagePlan.getPlan())));
    }
    return new Result(Lists.reverse(results));
  }

  @Value
  public static class Result {

    List<StageResult> results;

  }

  @Value
  public static class StageResult {

    ExecutionStage stage;
    ExecutionResult result;

  }

  public String executeFlink(FlinkStreamPhysicalPlan flinkPlan) {
    StatementSet statementSet = flinkPlan.getStatementSet();
    TableResult rslt = statementSet.execute();
    rslt.print(); //todo: this just forces print to wait for the async
    return rslt.getJobClient().get()
        .getJobID().toString();
  }
}
