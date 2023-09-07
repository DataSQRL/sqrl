package com.datasqrl;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.schema.SqrlListUtil;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.hints.OptimizerHint;
import com.datasqrl.plan.local.generate.SqrlTableNamespaceObject;
import com.datasqrl.plan.rules.AnnotatedLP;
import com.datasqrl.plan.rules.IdealExecutionStage;
import com.datasqrl.plan.rules.LPAnalysis;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.rules.SQRLConverter.Config;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.util.SqlNameUtil;
import com.datasqrl.util.StreamUtil;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqrlTableFunctionDef;
import org.apache.calcite.tools.RelBuilder;

public class SqrlPlanningTableFactory implements SqrlTableFactory {

  private final SqrlFramework framework;
  private final NameCanonicalizer nameCanonicalizer;

  public SqrlPlanningTableFactory(SqrlFramework framework, NameCanonicalizer nameCanonicalizer) {
    this.framework = framework;
    this.nameCanonicalizer = nameCanonicalizer;
  }

  @Override
  public void createTable(List<String> path, RelNode input, List<RelHint> hints,
      boolean setFieldNames, Optional<SqlNodeList> opHints, SqrlTableFunctionDef args) {
    framework.resetPlanner();
    LPAnalysis analyzedLP = convertToVanillaSQL(
        input, setFieldNames, framework.getQueryPlanner().getRelBuilder(),
        opHints, ErrorCollector.root());

    NamePath names = new SqlNameUtil(nameCanonicalizer).toNamePath(path);
    Optional<SQRLTable> parent = names.size() == 1
        ? Optional.empty()
        : Optional.of(framework.getSchema().getSqrlTable(names.popLast()));

    SqrlTableNamespaceObject nsObj = new CalciteTableFactory(framework, nameCanonicalizer)
        .createTable(names, analyzedLP, parent, args);

    nsObj.apply(Optional.empty(), framework, ErrorCollector.root());
  }

  //Converts SQRL statements into vanilla SQL
  public static LPAnalysis convertToVanillaSQL(RelNode relNode, boolean setOriginalFieldnames,
      RelBuilder relBuilder, Optional<SqlNodeList> hints, ErrorCollector errors) {
    //Parse all optimizer hints
    List<OptimizerHint> optimizerHints = OptimizerHint.fromSqlHint(hints, errors);
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
