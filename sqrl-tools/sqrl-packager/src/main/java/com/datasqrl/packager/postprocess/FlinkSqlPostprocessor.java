package com.datasqrl.packager.postprocess;

import static com.datasqrl.PlanConstants.PLAN_CONFIG;
import static com.datasqrl.PlanConstants.PLAN_SEPARATOR;
import static com.datasqrl.PlanConstants.PLAN_SQL;

import com.datasqrl.FlinkExecutablePlan;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SqlNodeToString;
import com.datasqrl.calcite.convert.SqlToStringFactory;
import com.datasqrl.engine.stream.flink.plan.FlinkStreamPhysicalPlan;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.sql.FlinkSqlGenerator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlSet;

/**
 */
public class FlinkSqlPostprocessor implements Postprocessor {

  @Override
  public void process(ProcessorContext context) {
    FlinkSqlGenerator flinkSqlGenerator = new FlinkSqlGenerator();
    FlinkExecutablePlan executablePlan = context.getCompilerResult().getPlan()
        .getPlans(FlinkStreamPhysicalPlan.class).findFirst().get()
        .getExecutablePlan();
    try {
      List<SqlNode> flinkSql = executablePlan.accept(flinkSqlGenerator,
          new FlinkSqlGenerator.FlinkSqlContext());
      SqlNodeToString sqlNodeToString = SqlToStringFactory.get(Dialect.CALCITE);
      Map<String, String> config = new LinkedHashMap<>();
      List<String> plan = new ArrayList<>();
      for (SqlNode sqlNode : flinkSql) {
        if (sqlNode instanceof SqlSet) {
          SqlSet set = (SqlSet)sqlNode;
          config.put(set.getKeyString(), set.getValueString());
        } else {
          plan.add(sqlNodeToString.convert(() -> sqlNode).getSql() + ";");
        }
      }
      Path planPath = context.getTargetDir().resolve(PLAN_SQL);
      Path configPath = context.getTargetDir().resolve(PLAN_CONFIG);
      new Deserializer().writeYML(configPath, config);
      Files.writeString(planPath, String.join(PLAN_SEPARATOR, plan));

    } catch (Exception e) {
      //allowed to fail, fallback on legacy flink-plan.json
    }
  }
}
