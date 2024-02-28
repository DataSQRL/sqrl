package com.datasqrl.plan;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.canonicalizer.Name;
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
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.plan.util.SelectIndexMap;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.util.SqlNameUtil;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.tools.RelBuilder;

@AllArgsConstructor(onConstructor_=@Inject)
public class SqrlPlanningTableFactory implements SqrlTableFactory {

  private final SqrlFramework framework;
  private final SqlNameUtil nameUtil;
  private final CalciteTableFactory tableFactory;

  @Override
  public void createTable(List<String> path, RelNode input, List<RelHint> hints,
      Optional<SqlNodeList> opHints,
      List<FunctionParameter> parameters, List<Function> isA, boolean materializeSelf,
      Optional<Supplier<RelNode>> relNodeSupplier, ErrorCollector errors) {
    LPAnalysis analyzedLP = convertToVanillaSQL(
        input, framework.getQueryPlanner().getRelBuilder(),
        opHints, errors);

    NamePath names = nameUtil.toNamePath(path);

    AnnotatedLP processedRel = analyzedLP.getConvertedRelnode();
    List<String> relFieldNames = processedRel.getRelNode().getRowType().getFieldNames();
    List<Name> fieldNames = processedRel.getSelect().targetsAsList().stream()
        .map(idx -> relFieldNames.get(idx))
        .map(n -> nameUtil.toName(n)).collect(Collectors.toList());

    Map<NamePath, ScriptRelationalTable> scriptTableDefinition = defineTable(names, analyzedLP, fieldNames);

    SqrlTableNamespaceObject nsObj = new SqrlTableNamespaceObject(names.getLast(),
        new ScriptTableDefinition(scriptTableDefinition),
        tableFactory, null, parameters, isA, materializeSelf, parameters, relNodeSupplier);

    nsObj.apply(Optional.empty(), framework, errors);
  }


  private LinkedHashMap<Integer, Name> mapFieldNamesToIndices(LPAnalysis analyzedLP, List<Name> fieldNames) {
    LinkedHashMap<Integer, Name> index2Name = new LinkedHashMap<>();
    SelectIndexMap selectMap = analyzedLP.getConvertedRelnode().getSelect();

    for (int i = 0; i < fieldNames.size(); i++) {
      index2Name.put(selectMap.map(i), fieldNames.get(i));
    }

    return index2Name;
  }

  public Map<NamePath, ScriptRelationalTable> defineTable(NamePath tablePath, LPAnalysis analyzedLP, List<Name> fieldNames) {
    // Validate the field names with the select map from the analyzed relational node
    validateFieldNames(analyzedLP, fieldNames);

    // Create base table from the analyzed LP
    PhysicalRelationalTable baseTable = tableFactory.createPhysicalRelTable(tablePath.getLast(), analyzedLP);

    // Convert field names to a linked hash map with corresponding indices
    LinkedHashMap<Integer, Name> index2Name = mapFieldNamesToIndices(analyzedLP, fieldNames);

    // Convert the base table's data type to a universal table
    UniversalTable rootTable = tableFactory.getTableConverter().convert2TableBuilder(
        tablePath,
        baseTable.getRowType(),
        baseTable.getNumPrimaryKeys(),
        index2Name
    );
    //Currently, we do NOT preserve the order of the fields as originally defined by the user in the script.
    //This may not be an issue, but if we need to preserve the order, it is probably easiest to re-order the fields
    //of tblDef.getTable() based on the provided list of fieldNames
    return tableFactory.createScriptTables(rootTable, baseTable);
  }

  private void validateFieldNames(LPAnalysis analyzedLP, List<Name> fieldNames) {
    SelectIndexMap selectMap = analyzedLP.getConvertedRelnode().getSelect();
    Preconditions.checkArgument(fieldNames.size() == selectMap.getSourceLength());
  }


  //Converts SQRL statements into vanilla SQL
  public static LPAnalysis convertToVanillaSQL(RelNode relNode,
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
    Config baseConfig = configBuilder.build();

    //Config for original construction without a specific stage
    configBuilder.stage(IdealExecutionStage.INSTANCE);
    Config config = configBuilder.build();

    SQRLConverter sqrlConverter = new SQRLConverter(relBuilder);
    AnnotatedLP alp = sqrlConverter.convert(relNode, config, errors);
    return new LPAnalysis(relNode, alp, configuredStages, baseConfig);
  }
}
