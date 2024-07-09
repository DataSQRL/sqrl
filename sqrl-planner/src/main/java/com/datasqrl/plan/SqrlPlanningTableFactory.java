package com.datasqrl.plan;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.hints.OptimizerHint;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;

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
    LPAnalysis analyzedLP = convertToVanillaSQL(
        input, framework.getQueryPlanner().getRelBuilder(),
        opHints, errors);

    NamePath names = nameUtil.toNamePath(path);

    AnnotatedLP processedRel = analyzedLP.getConvertedRelnode();
    List<String> relFieldNames = processedRel.getRelNode().getRowType().getFieldNames();
    List<Name> fieldNames = processedRel.getSelect().targetsAsList().stream()
        .map(idx -> relFieldNames.get(idx))
        .map(n -> nameUtil.toName(n)).collect(Collectors.toList());

    PhysicalRelationalTable table = defineTable(names, analyzedLP, fieldNames);

    SqrlTableNamespaceObject nsObj = new SqrlTableNamespaceObject(names.getLast(),
        table,
        tableFactory, null, parameters, isA, materializeSelf, parameters, relNodeSupplier,
        moduleLoader, isTest);

    nsObj.apply(null, Optional.empty(), framework, errors);
  }


  private LinkedHashMap<Integer, Name> mapFieldNamesToIndices(LPAnalysis analyzedLP, List<Name> fieldNames) {
    LinkedHashMap<Integer, Name> index2Name = new LinkedHashMap<>();
    SelectIndexMap selectMap = analyzedLP.getConvertedRelnode().getSelect();

    for (int i = 0; i < fieldNames.size(); i++) {
      index2Name.put(selectMap.map(i), fieldNames.get(i));
    }

    return index2Name;
  }

  public PhysicalRelationalTable defineTable(NamePath tablePath, LPAnalysis analyzedLP, List<Name> fieldNames) {
    // Validate the field names with the select map from the analyzed relational node
    validateFieldNames(analyzedLP, fieldNames);

    // Create base table from the analyzed LP
    PhysicalRelationalTable baseTable = tableFactory.createPhysicalRelTable(tablePath, analyzedLP);

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
    Pair<List<OptimizerHint>,List<SqlHint>> analyzedHints = OptimizerHint.fromSqlHints(hints, errors);
    //TODO: put other SQLHints back on the relnode after we are done, i.e. pass them through
    SqrlConverterConfig.SqrlConverterConfigBuilder configBuilder = SqrlConverterConfig.builder();
    //Apply only generic optimizer hints (pipeline optimization happens in the DAGPlanner)
    StreamUtil.filterByClass(analyzedHints.getKey(), OptimizerHint.Generic.class)
        .forEach(h -> h.add2Config(configBuilder, errors));
    //Capture stages
    List<OptimizerHint.Stage> configuredStages = StreamUtil.filterByClass(analyzedHints.getKey(),
        OptimizerHint.Stage.class).collect(Collectors.toList());
    SqrlConverterConfig baseConfig = configBuilder.build();

    //Config for original construction without a specific stage
    configBuilder.stage(IdealExecutionStage.INSTANCE);
    SqrlConverterConfig config = configBuilder.build();

    AnnotatedLP alp = sqrlConverter.convert(relNode, config, errors);
    return new LPAnalysis(relNode, alp, configuredStages, baseConfig);
  }
}
