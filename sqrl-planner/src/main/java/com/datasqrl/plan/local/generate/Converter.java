package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.calcite.OptimizationStage;
import com.datasqrl.plan.calcite.hints.OptimizerHint;
import com.datasqrl.plan.calcite.rules.AnnotatedLP;
import com.datasqrl.plan.calcite.rules.IdealExecutionStage;
import com.datasqrl.plan.calcite.rules.LPAnalysis;
import com.datasqrl.plan.calcite.rules.SQRLConverter;
import com.datasqrl.plan.calcite.rules.SQRLConverter.Config;
import com.datasqrl.util.StreamUtil;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.tools.RelBuilder;

public class Converter {

  public LPAnalysis convert(SqrlQueryPlanner planner, RelNode relNode, boolean setOriginalFieldnames,
      Namespace ns, Optional<SqlNodeList> hints, ErrorCollector errors) {
//    List<String> fieldNames = new ArrayList<>(relNode.getRowType().getFieldNames());

    //Push filters into joins so we can correctly identify self-joins
    relNode = planner.runStage(OptimizationStage.PUSH_FILTER_INTO_JOIN, relNode);

    //Convert all special SQRL conventions into vanilla SQL and remove self-joins
    //(including nested self-joins) as well as infer primary keys, table types, and
    //timestamps in the process
    return convertToVanillaSQL(ns, relNode, setOriginalFieldnames, planner.createRelBuilder(), hints, errors);
  }

  //Converts SQRL conventions into vanilla SQL
  private LPAnalysis convertToVanillaSQL(Namespace ns, RelNode relNode, boolean setOriginalFieldnames,
      RelBuilder relBuilder, Optional<SqlNodeList> hints, ErrorCollector errors) {
    //Parse all optimizer hints
    List<OptimizerHint> optimizerHints = OptimizerHint.fromSqlHint(hints);
    SQRLConverter.Config.ConfigBuilder configBuilder = SQRLConverter.Config.builder();
    //Apply only generic optimizer hints (pipeline optimization happens in the DAGPlanner)
    StreamUtil.filterByClass(optimizerHints, OptimizerHint.Generic.class)
        .forEach(h -> h.add2Config(configBuilder, errors));
    //Capture stages
    List<OptimizerHint.Stage> configuredStages = StreamUtil.filterByClass(optimizerHints,
        OptimizerHint.Stage.class).collect(Collectors.toList());
    configBuilder.setOriginalFieldnames(setOriginalFieldnames);
    Config baseConfig = configBuilder.build();

    //Config for original construction without a specific stage
    configBuilder.stage(IdealExecutionStage.INSTANCE);
    configBuilder.addTimestamp2NormalizedChildTable(false);
    Config config = configBuilder.build();

    SQRLConverter sqrlConverter = new SQRLConverter(relBuilder);
    AnnotatedLP alp = sqrlConverter.convert(relNode, config, errors);
    return new LPAnalysis(relNode, alp, configuredStages, baseConfig);
  }
}
