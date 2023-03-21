package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.name.Name;
import com.datasqrl.plan.calcite.OptimizationStage;
import com.datasqrl.plan.calcite.hints.ExecutionHint;
import com.datasqrl.plan.calcite.rules.AnnotatedLP;
import com.datasqrl.plan.calcite.rules.SQRLLogicalPlanConverter;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.tools.RelBuilder;

public class Converter {

  public AnnotatedLP convert(SqrlQueryPlanner planner, RelNode relNode, Namespace ns,
      Optional<SqlNodeList> hints, ErrorCollector errors) {
//    List<String> fieldNames = new ArrayList<>(relNode.getRowType().getFieldNames());

    //Push filters into joins so we can correctly identify self-joins
    relNode = planner.runStage(OptimizationStage.PUSH_FILTER_INTO_JOIN, relNode);

    //Convert all special SQRL conventions into vanilla SQL and remove self-joins
    //(including nested self-joins) as well as infer primary keys, table types, and
    //timestamps in the process
    AnnotatedLP prel = convertToVanillaSQL(ns, relNode, planner.createRelBuilder(), hints, errors);

    return prel;
  }

  //Converts SQRL conventions into vanilla SQL
  private AnnotatedLP convertToVanillaSQL(Namespace ns, RelNode relNode, RelBuilder relBuilder,
      Optional<SqlNodeList> hints, ErrorCollector errors) {
    final SQRLLogicalPlanConverter.Config.ConfigBuilder configBuilder = SQRLLogicalPlanConverter.Config.builder();
    Optional<ExecutionHint> execHint = ExecutionHint.fromSqlHint(hints);
    execHint.map(h -> h.getConfig(ns.getSchema().getPipeline(), configBuilder));
    SQRLLogicalPlanConverter.Config config = configBuilder.build();

    try {
      if (config.getStartStage() != null) {
        return SQRLLogicalPlanConverter.convert(relNode, () -> relBuilder, config);
      } else {
        return SQRLLogicalPlanConverter.findCheapest(Name.system("/*lp table*/").toNamePath(),
            relNode,  () -> relBuilder, ns.getSchema().getPipeline(), config);
      }
    } catch (Throwable e) {
      throw errors.handle(e);
    }
  }
}
