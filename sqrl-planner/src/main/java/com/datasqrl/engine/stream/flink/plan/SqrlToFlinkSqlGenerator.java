package com.datasqrl.engine.stream.flink.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.FlinkRelToSqlConverter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.plan.metadata.FlinkDefaultRelMetadataProvider;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.TableConfig;
import com.datasqrl.datatype.DataTypeMapper;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.stream.flink.connector.FlinkConnectorDataTypeMappingFactory;
import com.datasqrl.engine.stream.flink.sql.ExtractUniqueSourceVisitor;
import com.datasqrl.engine.stream.flink.sql.FlinkRelToSqlNode;
import com.datasqrl.engine.stream.flink.sql.rules.ExpandNestedTableFunctionRule.ExpandNestedTableFunctionRuleConfig;
import com.datasqrl.engine.stream.flink.sql.rules.ExpandTemporalJoinRule;
import com.datasqrl.engine.stream.flink.sql.rules.ExpandWindowHintRule.ExpandWindowHintRuleConfig;
import com.datasqrl.engine.stream.flink.sql.rules.ShapeBushyCorrelateJoinRule.ShapeBushyCorrelateJoinRuleConfig;
import com.datasqrl.plan.global.PhysicalDAGPlan.DatabaseStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.ExternalSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.Query;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.WriteQuery;
import com.datasqrl.plan.global.PhysicalDAGPlan.WriteSink;
import com.datasqrl.plan.table.ImportedRelationalTable;
import com.google.common.base.Preconditions;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@AllArgsConstructor
@Slf4j
public class SqrlToFlinkSqlGenerator {

  private final ExtractUniqueSourceVisitor uniqueSourceExtractor = new ExtractUniqueSourceVisitor();
  private final FlinkRelToSqlNode toSql = new FlinkRelToSqlNode();
  private final SqrlFramework framework;

  public SqlResult plan(List<? extends Query> stageQueries, List<StagePlan> stagePlans) {
    checkPreconditions(stageQueries);
    var writeQueries = applyFlinkCompatibilityRules(stageQueries);
    var sinksAndSources = extractTableDescriptors(writeQueries);
    List<SqlCall> stubSinks = new ArrayList<>();
    var stubSources = extractStubSources(writeQueries);

    List<SqlCreateView> queries = new ArrayList<>();
    List<RichSqlInsert> inserts = new ArrayList<>();
    Map<String, String> downcastClassNames = new HashMap<>();

    for (WriteQuery query : writeQueries) {
      var tableConfig = getTableConfig(query.getSink());
      if (query.getType().isState() && tableConfig.getConnectorConfig().getTableType().isStream()) {
//        log.warn("Attempting to write a stream to a state table. This may fail at runtime.");
      }
      var mappingFactory = new FlinkConnectorDataTypeMappingFactory();
      var connectorMapping = mappingFactory.getConnectorMapping(tableConfig);
      var relNode = applyDowncasting(framework.getQueryPlanner().getRelBuilder(),
          query.getExpandedRelNode(), query.getSink(), downcastClassNames, connectorMapping);
      var result = process(query.getSink().getName(), relNode);
      var sqlCreateTable = registerSinkTable(query.getSink(), relNode, stagePlans);
      var subSink = extractStubSinks(query.getSink(), relNode, stagePlans);
      stubSinks.add(subSink);
      sinksAndSources.add(sqlCreateTable);
      queries.addAll(result.getKey());
      inserts.add(result.getValue());
    }

    var functions = extractFunctions(writeQueries, downcastClassNames);
    return new SqlResult(sinksAndSources, ListUtils.union(stubSources, stubSinks), inserts, queries, functions);
  }

  private Pair<List<SqlCreateView>, RichSqlInsert> process(String name, RelNode relNode) {
    var convert = toSql.convert(relNode);
    var topLevelQuery = convert.getSqlNode();
    List<SqlCreateView> queries = convert.getQueryList().stream()
        .map(q -> FlinkSqlNodeFactory.createView(q.getTableName(), q.getNode()))
        .collect(Collectors.toList());

    var relToSqlConverter = new FlinkRelToSqlConverter(toSql.getAtomicInteger());
    var queryPipelineItem = relToSqlConverter.create(topLevelQuery);
    var query = FlinkSqlNodeFactory.createView(queryPipelineItem.getTableName(),
        queryPipelineItem.getNode());
    queries.add(query);

    var select = new SqlSelectBuilder().setFrom(query.getViewName()).build();

    return Pair.of(queries, FlinkSqlNodeFactory.createInsert(select, name));
  }

  private RelNode applyDowncasting(RelBuilder relBuilder, RelNode relNode, WriteSink writeSink,
      Map<String, String> downcastClassNames, Optional<DataTypeMapper> connectorMapping) {
    relBuilder.push(relNode);
    var hasChanged = new AtomicBoolean();

    List<RexNode> fields = relNode.getRowType().getFieldList().stream().map(
            field -> convertField(field, hasChanged, relBuilder, downcastClassNames, connectorMapping, relNode))
        .collect(Collectors.toList());

    if (hasChanged.get()) {
      return relBuilder.project(fields, relNode.getRowType().getFieldNames(), true).build();
    }
    return relNode;
  }

  private RexNode convertField(RelDataTypeField field, AtomicBoolean hasChanged,
      RelBuilder relBuilder, Map<String, String> downcastClassNames,
      Optional<DataTypeMapper> connectorMapping, RelNode relNode) {
    if (connectorMapping.isEmpty() || connectorMapping.get().nativeTypeSupport(field.getType())) {
      return relBuilder.field(field.getIndex());
    }

    var downcastFunction = connectorMapping.get().convertType(field.getType());
    if (downcastFunction.isEmpty()) {
      throw new RuntimeException(
          "Could not find downcast function for: " + field.getType().getFullTypeString());
    }

    var castFunction = downcastFunction.get();
    downcastClassNames.put(castFunction.getFunction().getClass().getSimpleName(), castFunction.getClassName());

    framework.getFlinkFunctionCatalog().registerCatalogFunction(
        UnresolvedIdentifier.of(castFunction.getFunction().getClass().getSimpleName()),
        castFunction.getFunction().getClass(), true);

    hasChanged.set(true);

    List<SqlOperator> list = new ArrayList<>();
    framework.getSqrlOperatorTable()
        .lookupOperatorOverloads(new SqlIdentifier(castFunction.getFunction().getClass().getSimpleName(), SqlParserPos.ZERO),
            SqlFunctionCategory.USER_DEFINED_FUNCTION,
            SqlSyntax.FUNCTION, list, SqlNameMatchers.liberal());

    return relBuilder.getRexBuilder()
        .makeCall(list.get(0), List.of(relBuilder.field(field.getIndex())));
  }

  private TableConfig getTableConfig(WriteSink sink) {
    if (sink instanceof EngineSink engineSink) {
      return engineSink.getStage().getEngine().getSinkConfig(engineSink.getNameId());
    } else if (sink instanceof ExternalSink externalSink) {
      return externalSink.getTableSink().getConfiguration();
    } else {
      throw new RuntimeException("Could not get format for sink");
    }
  }

  private List<SqlCreateFunction> extractFunctions(List<WriteQuery> writeQueries,
      Map<String, String> downcastClassNames) {
    Map<String, String> mutableUdfs = framework.getSchema().getUdf().entrySet().stream()
            .collect(
            Collectors.toMap(Map.Entry::getKey, e -> extractFunctionClass(e.getValue()).getName()));

    mutableUdfs.putAll(downcastClassNames);
    mutableUdfs.remove("NOW".toLowerCase());

    return mutableUdfs.entrySet().stream()
        .map(entry -> FlinkSqlNodeFactory.createFunction(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  private Class<?> extractFunctionClass(UserDefinedFunction o) {
    return o.getClass();
  }

  private Set<SqlCall> extractTableDescriptors(List<WriteQuery> queries) {
    var tables = uniqueSourceExtractor.extract(queries);

    Set<SqlCall> sources = new LinkedHashSet<>();
    for (Map.Entry<String, ImportedRelationalTable> entry : tables.entrySet()) {
      var tableName = entry.getKey();
      var table = entry.getValue();
      TableConfig tableConfig = table.getTableSource().getConfiguration();

      var sqlCreateTable = FlinkSqlNodeFactory.createTable(
          tableName,
          table.getRowType(),
          tableConfig.getBase().getPartitionKey(),
          tableConfig.getBase().getWatermarkMillis(),
          tableConfig.getBase().getTimestampColumn().map(NamePath::parse)
              .map(NamePath::getLast)
              .map(Name::getDisplay),
          tableConfig.getMetadataConfig().toMap(),
          tableConfig.getPrimaryKeyConstraint(),
          new HashMap<>(tableConfig.getConnectorConfig().toMap()),
          e -> framework.getQueryPlanner().parseCall(e)
      );

      sources.add(sqlCreateTable);
    }

    return sources;
  }
  private List<SqlCall> extractStubSources(List<WriteQuery> queries) {
    var tables = uniqueSourceExtractor.extract(queries);

    List<SqlCall> sources = new ArrayList<>();
    for (Map.Entry<String, ImportedRelationalTable> entry : tables.entrySet()) {
      var tableName = entry.getKey();
      var table = entry.getValue();
      TableConfig tableConfig = table.getTableSource().getConfiguration();

      var sqlCreateTable = FlinkSqlNodeFactory.createTable(
          tableName,
          table.getRowType(),
          tableConfig.getBase().getPartitionKey(),
          tableConfig.getBase().getWatermarkMillis(),
          tableConfig.getBase().getTimestampColumn().map(NamePath::parse)
              .map(NamePath::getLast)
              .map(Name::getDisplay),
          Map.of(),
          tableConfig.getPrimaryKeyConstraint(),
          Map.of("connector", "datagen"),
          e -> framework.getQueryPlanner().parseCall(e)
      );

      sources.add(sqlCreateTable);
    }

    return sources;
  }

  private SqlCreateTable registerSinkTable(WriteSink sink, RelNode relNode, List<StagePlan> stagePlans) {
    String name;
    TableConfig tableConfig;

    if (sink instanceof EngineSink engineSink) {
      var engineConfig = engineSink.getStage().getEngine().getSinkConfig(engineSink.getNameId());

      var stagePlan = stagePlans.stream()
          .filter(f -> f.getStage() == engineSink.getStage())
          .findFirst()
          .orElseThrow();

      var configBuilder = engineConfig.toBuilder();

      if (engineSink.getStage().supportsFeature(EngineFeature.PARTITIONING)) {
        var dbPlan = (DatabaseStagePlan) stagePlan;
        var tableId = engineSink.getNameId();
        var optIndex = dbPlan.getIndexDefinitions().stream()
            .filter(idx -> idx.getTableId().equals(tableId))
            .filter(idx -> idx.getType().isPartitioned())
            .findFirst();
        optIndex.ifPresent(partitionIndex -> {
          var partitionColumns = partitionIndex.getColumnNames()
              .subList(0, partitionIndex.getPartitionOffset());
          if (!partitionColumns.isEmpty()) {
            configBuilder.setPartitionKey(partitionColumns);
          }
        });
      }

      var pks = IntStream.of(engineSink.getPrimaryKeys())
          .mapToObj(i -> relNode.getRowType().getFieldList().get(i).getName())
          .toArray(String[]::new);
      configBuilder.setPrimaryKey(pks);

      tableConfig = configBuilder.build();
      name = engineSink.getNameId();
    } else if (sink instanceof ExternalSink externalSink) {
      tableConfig = externalSink.getTableSink().getConfiguration();
      name = externalSink.getName();
    } else {
      throw new RuntimeException("Could not identify write sink type.");
    }

    var sqlCreateTable = FlinkSqlNodeFactory.createTable(
        name,
        relNode.getRowType(),
        tableConfig.getBase().getPartitionKey(),
        -1,
        Optional.empty(),
        tableConfig.getMetadataConfig().toMap(),
        tableConfig.getPrimaryKeyConstraint(),
        new HashMap<>(tableConfig.getConnectorConfig().toMap()),
        e -> framework.getQueryPlanner().parseCall(e)
    );

    return sqlCreateTable;
  }

  private SqlCreateTable extractStubSinks(WriteSink sink, RelNode relNode, List<StagePlan> stagePlans) {
    String name;
    TableConfig tableConfig;

    if (sink instanceof EngineSink engineSink) {
      var engineConfig = engineSink.getStage().getEngine().getSinkConfig(engineSink.getNameId());

      var stagePlan = stagePlans.stream()
          .filter(f -> f.getStage() == engineSink.getStage())
          .findFirst()
          .orElseThrow();

      var configBuilder = engineConfig.toBuilder();

      if (engineSink.getStage().supportsFeature(EngineFeature.PARTITIONING)) {
        var dbPlan = (DatabaseStagePlan) stagePlan;
        var tableId = engineSink.getNameId();
        var optIndex = dbPlan.getIndexDefinitions().stream()
            .filter(idx -> idx.getTableId().equals(tableId))
            .filter(idx -> idx.getType().isPartitioned())
            .findFirst();
        optIndex.ifPresent(partitionIndex -> {
          var partitionColumns = partitionIndex.getColumnNames()
              .subList(0, partitionIndex.getPartitionOffset());
          if (!partitionColumns.isEmpty()) {
            configBuilder.setPartitionKey(partitionColumns);
          }
        });
      }

      var pks = IntStream.of(engineSink.getPrimaryKeys())
          .mapToObj(i -> relNode.getRowType().getFieldList().get(i).getName())
          .toArray(String[]::new);
      configBuilder.setPrimaryKey(pks);

      tableConfig = configBuilder.build();
      name = engineSink.getNameId();
    } else if (sink instanceof ExternalSink externalSink) {
      tableConfig = externalSink.getTableSink().getConfiguration();
      name = externalSink.getName();
    } else {
      throw new RuntimeException("Could not identify write sink type.");
    }

    var sqlCreateTable = FlinkSqlNodeFactory.createTable(
        name,
        relNode.getRowType(),
        tableConfig.getBase().getPartitionKey(),
        -1,
        Optional.empty(),
        tableConfig.getMetadataConfig().toMap(),
        tableConfig.getPrimaryKeyConstraint(),
        Map.of("connector", "blackhole"),
        e -> framework.getQueryPlanner().parseCall(e)
    );

    return sqlCreateTable;
  }

  private void checkPreconditions(List<? extends Query> queries) {
    queries.forEach(query -> Preconditions.checkState(query instanceof WriteQuery,
        "Unexpected query type when creating executable plan"));
  }

  private List<WriteQuery> applyFlinkCompatibilityRules(List<? extends Query> queries) {
    return queries.stream().map(q -> applyFlinkCompatibilityRules((WriteQuery) q))
        .collect(Collectors.toList());
  }

  private WriteQuery applyFlinkCompatibilityRules(WriteQuery query) {
    return new WriteQuery(query.getSink(), applyFlinkCompatibilityRules(query.getExpandedRelNode()),
        applyFlinkCompatibilityRules(query.getRelNode()), query.getType());
  }

  private RelNode applyFlinkCompatibilityRules(RelNode relNode) {
    var program = Programs.hep(
        List.of(new ExpandTemporalJoinRule(), ExpandWindowHintRuleConfig.DEFAULT.toRule(),
            ShapeBushyCorrelateJoinRuleConfig.DEFAULT.toRule(),
            ExpandNestedTableFunctionRuleConfig.DEFAULT.toRule()), false,
        FlinkDefaultRelMetadataProvider.INSTANCE());

    return program.run(null, relNode, relNode.getTraitSet(), List.of(), List.of());
  }

  @lombok.Value
  public static class SqlResult {

    private Set<SqlCall> sinksSources;
    private List<SqlCall> stubSinksSources;
    private List<RichSqlInsert> inserts;
    private List<SqlCreateView> queries;
    private List<SqlCreateFunction> functions;

    public List<SqlNode> getStubSchema() {
      return ListUtils.union(functions, stubSinksSources);
    }
  }
}
