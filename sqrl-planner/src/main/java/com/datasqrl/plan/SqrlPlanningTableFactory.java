package com.datasqrl.plan;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.tools.RelBuilder;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.hints.OptimizerHint;
import com.datasqrl.plan.hints.OptimizerHint.ConverterHint;
import com.datasqrl.plan.hints.PipelineStageHint;
import com.datasqrl.plan.local.generate.SqrlTableNamespaceObject;
import com.datasqrl.plan.rules.AnnotatedLP;
import com.datasqrl.plan.rules.IdealExecutionStage;
import com.datasqrl.plan.rules.LPAnalysis;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.rules.SqrlConverterConfig;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.util.SelectIndexMap;
import com.datasqrl.util.SqlNameUtil;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_=@Inject)
public class SqrlPlanningTableFactory implements SqrlTableFactory {

  private final SqrlFramework framework;
  private final SqlNameUtil nameUtil;
  private final CalciteTableFactory tableFactory;
  private final SQRLConverter sqrlConverter;

  @Override
  public void createTable(ModuleLoader moduleLoader, List<String> path, RelNode input, List<RelHint> hints,
      Optional<SqlNodeList> opHints,
      List<FunctionParameter> parameters, List<Function> isA, boolean materializeSelf,
      Optional<Supplier<RelNode>> relNodeSupplier, ErrorCollector errors, boolean isTest) {
    var analyzedLP = convertToVanillaSQL(
        input, framework.getQueryPlanner().getRelBuilder(),
        opHints, errors);

    var names = nameUtil.toNamePath(path);

    AnnotatedLP processedRel = analyzedLP.getConvertedRelnode();
    var relFieldNames = processedRel.getRelNode().getRowType().getFieldNames();
    List<Name> fieldNames = processedRel.getSelect().targetsAsList().stream()
        .map(idx -> relFieldNames.get(idx))
        .map(n -> nameUtil.toName(n)).collect(Collectors.toList());

    var table = defineTable(names, analyzedLP, fieldNames);

    var nsObj = new SqrlTableNamespaceObject(names.getLast(),
        table,
        tableFactory, null, parameters, isA, materializeSelf, parameters, relNodeSupplier,
        moduleLoader, isTest, opHints);

    nsObj.apply(null, Optional.empty(), framework, errors);
  }


  private LinkedHashMap<Integer, Name> mapFieldNamesToIndices(LPAnalysis analyzedLP, List<Name> fieldNames) {
    var index2Name = new LinkedHashMap<Integer, Name>();
    SelectIndexMap selectMap = analyzedLP.getConvertedRelnode().getSelect();

    for (var i = 0; i < fieldNames.size(); i++) {
      index2Name.put(selectMap.map(i), fieldNames.get(i));
    }

    return index2Name;
  }

  public PhysicalRelationalTable defineTable(NamePath tablePath, LPAnalysis analyzedLP, List<Name> fieldNames) {
    // Validate the field names with the select map from the analyzed relational node
    validateFieldNames(analyzedLP, fieldNames);

    // Create base table from the analyzed LP
    var baseTable = tableFactory.createPhysicalRelTable(tablePath, analyzedLP);

    //Currently, we do NOT preserve the order of the fields as originally defined by the user in the script.
    //This may not be an issue, but if we need to preserve the order, it is probably easiest to re-order the fields
    //of tblDef.getTable() based on the provided list of fieldNames
    return baseTable;
  }

  private void validateFieldNames(LPAnalysis analyzedLP, List<Name> fieldNames) {
    SelectIndexMap selectMap = analyzedLP.getConvertedRelnode().getSelect();
    Preconditions.checkArgument(fieldNames.size() == selectMap.getSourceLength());
  }


  //Converts SQRL statements into vanilla SQL
  private LPAnalysis convertToVanillaSQL(RelNode relNode,
      RelBuilder relBuilder, Optional<SqlNodeList> hints, ErrorCollector errors) {
    //Parse all optimizer hints
    var analyzedHints = OptimizerHint.fromSqlHints(hints, errors);
    //TODO: put other SQLHints back on the relnode after we are done, i.e. pass them through
    var configBuilder = SqrlConverterConfig.builder();
    //Add default primary key if it has none
    configBuilder.addDefaultPrimaryKey(true);
    //Apply only generic optimizer hints (pipeline optimization happens in the DAGPlanner)
    StreamUtil.filterByClass(analyzedHints.getKey(), ConverterHint.class)
        .forEach(h -> h.add2Config(configBuilder, errors));
    //Capture stages
    List<PipelineStageHint> configuredStages = StreamUtil.filterByClass(analyzedHints.getKey(),
        PipelineStageHint.class).collect(Collectors.toList());
    var baseConfig = configBuilder.build();

    //Config for original construction without a specific stage
    configBuilder.stage(IdealExecutionStage.INSTANCE);
    var config = configBuilder.build();

    var alp = sqrlConverter.convert(relNode, config, errors);
    return new LPAnalysis(relNode, alp, configuredStages, baseConfig, analyzedHints.getKey(),
        analyzedHints.getValue());
  }
}
