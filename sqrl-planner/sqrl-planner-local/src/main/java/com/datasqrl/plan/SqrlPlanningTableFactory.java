package com.datasqrl.plan;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.calcite.schema.SqrlListUtil;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.hints.OptimizerHint;
import com.datasqrl.plan.local.ScriptTableDefinition;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.tools.RelBuilder;

public class SqrlPlanningTableFactory implements SqrlTableFactory {

  private final SqrlFramework framework;
  private final NameCanonicalizer nameCanonicalizer;
  private final SqlNameUtil nameUtil;

  public SqrlPlanningTableFactory(SqrlFramework framework, NameCanonicalizer nameCanonicalizer) {
    this.framework = framework;
    this.nameCanonicalizer = nameCanonicalizer;
    this.nameUtil = new SqlNameUtil(nameCanonicalizer);
  }

  @Override
  public void createTable(List<String> path, RelNode input, List<RelHint> hints,
      boolean setFieldNames, Optional<SqlNodeList> opHints,
      List<FunctionParameter> parameters, List<Function> isA, boolean materializeSelf,
      Optional<Supplier<RelNode>> relNodeSupplier, ErrorCollector errors) {
    framework.resetPlanner();
    LPAnalysis analyzedLP = convertToVanillaSQL(
        input, setFieldNames, framework.getQueryPlanner().getRelBuilder(),
        opHints, errors);

    NamePath names = nameUtil.toNamePath(path);

    Optional<SQRLTable> parent = Optional.empty();
    if (path.size() > 1) {
      TableFunction function = framework.getQueryPlanner()
          .getTableFunction(SqrlListUtil.popLast(path)).getFunction();
      SqrlTableMacro sqrlTable = (SqrlTableMacro)function;
      parent = Optional.of(sqrlTable.getSqrlTable());
    }

    List<SQRLTable> isATable = isA.stream()
        .map(f->(SqrlTableMacro) f)
        .map(SqrlTableMacro::getSqrlTable)
        .collect(Collectors.toList());

    AnnotatedLP processedRel = analyzedLP.getConvertedRelnode();

    List<String> relFieldNames = processedRel.getRelNode().getRowType().getFieldNames().stream()
        //todo: one versioned field comes in with a distinct on statement, fix it and remove this line
        .map(s->s.contains("$") ? s.split("\\$")[0] : s) //remove version info
        .collect(Collectors.toList());

    List<Name> fieldNames = processedRel.getSelect().targetsAsList().stream()
        .map(idx -> relFieldNames.get(idx))
        .map(n -> nameUtil.toName(n)).collect(Collectors.toList());

    ScriptTableDefinition scriptTableDefinition = new CalciteTableFactory(framework, nameCanonicalizer)
        .defineTable(names, analyzedLP, fieldNames, parent, materializeSelf, relNodeSupplier,
            Optional.of(parameters), Optional.of(isATable));

    SqrlTableNamespaceObject nsObj = new SqrlTableNamespaceObject(names.getLast(), scriptTableDefinition,
        null, null, parameters, isA, materializeSelf);

    nsObj.apply(Optional.empty(), framework, errors);
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
