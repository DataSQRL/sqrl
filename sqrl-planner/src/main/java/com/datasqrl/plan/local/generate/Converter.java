package com.datasqrl.plan.local.generate;

import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.name.Name;
import com.datasqrl.plan.calcite.OptimizationStage;
import com.datasqrl.plan.calcite.RelStageRunner;
import com.datasqrl.plan.calcite.hints.ExecutionHint;
import com.datasqrl.plan.calcite.rules.AnnotatedLP;
import com.datasqrl.plan.calcite.rules.SQRLLogicalPlanConverter;
import com.datasqrl.plan.local.generate.SqrlStatementVisitor.SystemContext;
import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.tools.RelBuilder;

public class Converter {

  private final SystemContext systemContext;

  public Converter(SystemContext systemContext) {

    this.systemContext = systemContext;
  }

  public AnnotatedLP convert(RelNode relNode, Namespace ns, boolean isStream,
      Optional<SqlNodeList> hints) {
//    List<String> fieldNames = new ArrayList<>(relNode.getRowType().getFieldNames());

    //Push filters into joins so we can correctly identify self-joins
    relNode = RelStageRunner.runStage(OptimizationStage.PUSH_FILTER_INTO_JOIN,
        relNode, ns.session.getRelPlanner());

    //Convert all special SQRL conventions into vanilla SQL and remove self-joins
    //(including nested self-joins) as well as infer primary keys, table types, and
    //timestamps in the process
    AnnotatedLP prel = convertToVanillaSQL(ns, relNode, ns.session.createRelBuilder(), isStream, hints);

    return prel;
  }

  //Converts SQRL conventions into vanilla SQL
  private AnnotatedLP convertToVanillaSQL(Namespace ns, RelNode relNode, RelBuilder relBuilder,
      boolean isStream, Optional<SqlNodeList> hints) {
    final SQRLLogicalPlanConverter.Config.ConfigBuilder configBuilder = SQRLLogicalPlanConverter.Config.builder();
    Optional<ExecutionHint> execHint = ExecutionHint.fromSqlHint(hints);
    if (isStream) {
      Preconditions.checkArgument(
          !execHint.filter(h -> h.getExecType() != ExecutionEngine.Type.STREAM).isPresent(),
          "Invalid execution hint: %s", execHint);
      if (execHint.isEmpty()) {
        execHint = Optional.of(new ExecutionHint(ExecutionEngine.Type.STREAM));
      }
    }
    execHint.map(h -> h.getConfig(ns.session.getPipeline(), configBuilder));
    SQRLLogicalPlanConverter.Config config = configBuilder.build();

    if (config.getStartStage() != null) {
      return SQRLLogicalPlanConverter.convert(relNode, () -> relBuilder, config);
    } else {
      return SQRLLogicalPlanConverter.findCheapest(Name.system("/*lp table*/").toNamePath(),
          relNode,  () -> relBuilder, ns.session.pipeline, config);
    }
  }

}
