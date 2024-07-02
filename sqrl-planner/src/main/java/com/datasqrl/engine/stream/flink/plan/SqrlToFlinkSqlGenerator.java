package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.DefaultFunctions;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;
import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.TableConfig.ConnectorConfig;
import com.datasqrl.config.TableConfig.Format;
import com.datasqrl.config.TableConfig.MetadataConfig;
import com.datasqrl.config.TableConfig.MetadataEntry;
import com.datasqrl.config.TableConfig;
import com.datasqrl.config.TableConfig.TableTableConfig;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.stream.flink.sql.ExtractUniqueSourceVisitor;
import com.datasqrl.engine.stream.flink.sql.FlinkRelToSqlNode;
import com.datasqrl.engine.stream.flink.sql.FlinkRelToSqlNode.FlinkSqlNodes;
import com.datasqrl.engine.stream.flink.sql.model.QueryPipelineItem;
import com.datasqrl.engine.stream.flink.sql.rules.ExpandNestedTableFunctionRule.ExpandNestedTableFunctionRuleConfig;
import com.datasqrl.engine.stream.flink.sql.rules.ExpandTemporalJoinRule;
import com.datasqrl.engine.stream.flink.sql.rules.ExpandWindowHintRule.ExpandWindowHintRuleConfig;
import com.datasqrl.engine.stream.flink.sql.rules.PushDownWatermarkHintRule.PushDownWatermarkHintConfig;
import com.datasqrl.engine.stream.flink.sql.rules.PushWatermarkHintToTableScanRule.PushWatermarkHintToTableScanConfig;
import com.datasqrl.engine.stream.flink.sql.rules.ShapeBushyCorrelateJoinRule.ShapeBushyCorrelateJoinRuleConfig;
import com.datasqrl.flink.FlinkConverter;
import com.datasqrl.function.DowncastFunction;
import com.datasqrl.io.schema.json.FlexibleJsonFlinkFormatFactory;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.ExternalSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.Query;
import com.datasqrl.plan.global.PhysicalDAGPlan.WriteQuery;
import com.datasqrl.plan.global.PhysicalDAGPlan.WriteSink;
import com.datasqrl.plan.table.ImportedRelationalTable;
import com.datasqrl.sql.SqlCallRewriter;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.FlinkRelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlConstraintEnforcement;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.constraint.SqlUniqueSpec;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.metadata.FlinkDefaultRelMetadataProvider;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

@Getter
@AllArgsConstructor
public class SqrlToFlinkSqlGenerator {
  private final ExtractUniqueSourceVisitor uniqueSourceExtractor = new ExtractUniqueSourceVisitor();
  final FlinkRelToSqlNode toSql = new FlinkRelToSqlNode();

  SqrlFramework framework;

  public SqlResult plan(List<? extends Query> stageQueries) {
    checkPreconditions(stageQueries);
    List<WriteQuery> writeQueries = applyFlinkCompatibilityRules(stageQueries);
    Set<SqlCreateTable> sinksAndSources = extractTableDescriptors(writeQueries);

    List<SqlCreateView> queries = new ArrayList<>();
    List<RichSqlInsert> inserts = new ArrayList<>();
    Map<String, String> downcastClassNames = new HashMap<>();

    for (WriteQuery query : writeQueries) {
      RelNode relNode = applyDowncasting(framework.getQueryPlanner().getRelBuilder(),
          query.getRelNode(), query.getSink(), downcastClassNames);
      Pair<List<SqlCreateView>, RichSqlInsert> result = process(query.getSink().getName(), relNode);
      SqlCreateTable sqlCreateTable = registerSinkTable(query.getSink(), relNode);
      sinksAndSources.add(sqlCreateTable);
      queries.addAll(result.getKey());
      inserts.add(result.getValue());
    }

    List<SqlCreateFunction> functions = extractFunctions(writeQueries, downcastClassNames);
    return new SqlResult(sinksAndSources, inserts, queries, functions);
  }

  private Pair<List<SqlCreateView>, RichSqlInsert> process(String name, RelNode relNode) {
    FlinkSqlNodes convert = toSql.convert(relNode);

    SqlNode topLevelQuery = convert.getSqlNode();
    List<SqlCreateView> queries = convert.getQueryList()
        .stream()
        .map(this::createQuery)
        .collect(Collectors.toList());

    FlinkRelToSqlConverter relToSqlConverter = new FlinkRelToSqlConverter(toSql.getAtomicInteger());
    QueryPipelineItem queryPipelineItem = relToSqlConverter.create(topLevelQuery);
    SqlCreateView query = createQuery(queryPipelineItem);
    queries.add(query);

    SqlSelect select = new SqlSelectBuilder()
        .setFrom(query.getViewName())
        .build();

    return Pair.of(queries, createInsert(select, name));
  }

  private SqlCreateView createQuery(QueryPipelineItem q) {
    return new SqlCreateView(SqlParserPos.ZERO,
        identifier(q.getTableName()),
        SqlNodeList.EMPTY,
        q.getNode(),
        false,
        false,
        false,
        null,
        null
    );
  }

  private RichSqlInsert createInsert(SqlNode source, String target) {
    return new RichSqlInsert(SqlParserPos.ZERO, SqlNodeList.EMPTY, SqlNodeList.EMPTY,
        identifier(target), source, null, null);
  }

  public static SqlIdentifier identifier(String str) {
    return new SqlIdentifier(str, SqlParserPos.ZERO);
  }
  private RelNode applyDowncasting(RelBuilder relBuilder, RelNode relNode, WriteSink writeSink,
      Map<String, String> downcastClassNames) {
    relBuilder.push(relNode);

    AtomicBoolean hasChanged = new AtomicBoolean();
    List<RexNode> fields = relNode.getRowType().getFieldList().stream()
        .map(field -> convertField(field, hasChanged, relBuilder, writeSink, downcastClassNames))
        .collect(Collectors.toList());

    if (hasChanged.get()) {
      return relBuilder.project(fields, relNode.getRowType().getFieldNames(), true).build();
    }
    return relNode;
  }

  private RexNode convertField(RelDataTypeField field, AtomicBoolean hasChanged,
      RelBuilder relBuilder, WriteSink writeSink, Map<String, String> downcastClassNames) {
    //If they are not raw, is basic type and return
//    if (!isRawType(field.getType())) {
//      return relBuilder.field(field.getIndex());
//    }

    //1. Check to see if format or engine supports converting the data type to a type flink can handle natively
    TableConfig tableConfig = getTableConfig(writeSink);

    //Check if format supports type: if yes then return
    if (formatSupportsType(tableConfig.getConnectorConfig(), field.getType())) {
      return relBuilder.field(field.getIndex());
    } else if (connectorSupportsType(writeSink, tableConfig.getConnectorConfig(), field.getType())) {
      return relBuilder.field(field.getIndex());
    }

    //Check if engine supports type (jdbc only?): if yes then return
    //Else: downcast

    //If it's a raw type and the format or engine supports it, return it
//    if (field.getType() instanceof RawRelDataType && (formatSupportsType(tableConfig.getConnectorConfig(), (RawRelDataType) field.getType())
//        || engineRequiresDowncasting(getEngine(writeSink),
//        field.getType()))
//        ((RawRelDataType) field.getType()).getRawType().getDefaultConversion()))
//    ) {
//      return relBuilder.field(field.getIndex());
    else {
      if (getEngine(writeSink).isEmpty()) {
        throw new RuntimeException("Needed downcast function but could not find one for RAW type: "
            + field.getType());
      }

      FlinkConverter flinkConverter = new FlinkConverter(
          (TypeFactory) framework.getQueryPlanner().getCatalogReader().getTypeFactory());

      DowncastFunction downcastFunction = getEngineDowncastFunction(
          getEngine(writeSink).get(), field.getType()).get();
      //Otherwise apply downcasting

//      Class<?> defaultConversion = ((RawRelDataType) field.getType()).getRawType()
//          .getDefaultConversion();

//      DowncastFunction downcastFunction = ServiceLoaderDiscovery.get(DowncastFunction.class,
//          e -> e.getConversionClass().getName(), defaultConversion.getName());

      String fncName = downcastFunction.downcastFunctionName().toLowerCase();
      FunctionDefinition functionDef;
      try {
        functionDef = (FunctionDefinition) downcastFunction.getDowncastClassName()
            .getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      Optional<SqlFunction> convertedFunction = flinkConverter.convertFunction(fncName,
          functionDef);

      if (convertedFunction.isEmpty()) {
        throw new RuntimeException("Could not convert downcast function");
      }

      downcastClassNames.put(fncName, downcastFunction.getDowncastClassName().getName());

      hasChanged.set(true);
//      return relBuilder.field(field.getIndex());
      return relBuilder.getRexBuilder()
          .makeCall(convertedFunction.get(), List.of(relBuilder.field(field.getIndex())));
    }
  }

  private boolean connectorSupportsType(WriteSink writeSink, ConnectorConfig connectorConfig, RelDataType type) {
    // check if its an engine
    if (getEngine(writeSink).isPresent()) {
      return getEngineDowncastFunction(getEngine(writeSink).get(), type).isEmpty();
    }

    //assume by default it does
    return true;

//
//      List<Class> conversionClasses = ServiceLoaderDiscovery.getAll(JdbcTypeSerializer.class).stream()
//          .map(JdbcTypeSerializer::getConversionClass)
//          .collect(Collectors.toList());
//
//      return conversionClasses.contains(relDataType.getRawType().getOriginatingClass());
  }

  private Optional<ExecutionEngine> getEngine(WriteSink sink) {
     if (sink instanceof EngineSink) {
       return Optional.of(((EngineSink) sink).getStage().getEngine());
     } else if (sink instanceof ExternalSink) {
       return Optional.empty();
     }
     throw new RuntimeException("Unsupported sink");
   }

   private Optional<DowncastFunction> getEngineDowncastFunction(ExecutionEngine engine, RelDataType datatype) {
     return engine.getSinkTypeCastFunction(datatype);
   }
  private boolean isRawType(RelDataType type) {
    return type instanceof RawRelDataType;
  }

  private boolean formatSupportsType(ConnectorConfig tableConfig, RelDataType type) {
    Optional<Format> format = tableConfig.getFormat();
    if (format.isEmpty()) {
      return false;
    }

    if (format.get().getName().equalsIgnoreCase(FlexibleJsonFlinkFormatFactory.FORMAT_NAME) && type instanceof RawRelDataType) {
      RawRelDataType relDataType = (RawRelDataType) type;
      return FlexibleJsonFlinkFormatFactory.getSupportedTypeClasses().contains(relDataType.getRawType().getOriginatingClass());
    } else {
      return !(type instanceof RawRelDataType);
    }
  }

  private TableConfig getTableConfig(WriteSink sink) {
    if (sink instanceof EngineSink) {
      EngineSink engineSink = (EngineSink) sink;
      return engineSink.getStage().getEngine().getSinkConfig(engineSink.getNameId());
    } else if (sink instanceof ExternalSink) {
      ExternalSink externalSink = (ExternalSink) sink;
      return externalSink.getTableSink().getConfiguration();
    } else {
      throw new RuntimeException("Could not get format for sink");
    }
  }

  private List<SqlCreateFunction> extractFunctions(List<WriteQuery> writeQueries,
      Map<String, String> downcastClassNames) {
    Map<String, String> mutableUdfs = framework.getSqrlOperatorTable().getUdfs().entrySet().stream()
        .filter(f->isUdf(f.getValue()))
        .collect(Collectors.toMap(Entry::getKey, e->extractFunctionClass(e.getValue()).getName()));

    mutableUdfs.putAll(downcastClassNames);
    //exclude sqrl NOW for flink's NOW
    mutableUdfs.remove(DefaultFunctions.NOW.getName().toLowerCase());

    List<SqlCreateFunction> functionList = new ArrayList<>();
    for (Map.Entry<String, String> fnc : mutableUdfs.entrySet()) {
      functionList.add(createFunction(fnc.getKey(), fnc.getValue()));
    }

    return functionList;
  }

  private boolean isUdf(SqlOperator o) {
    Class aClass = extractFunctionClass(o);
    if (aClass == BuiltInFunctionDefinition.class) return false;
    return UserDefinedFunction.class.isAssignableFrom(aClass);
  }

  private Class extractFunctionClass(SqlOperator o) {
    if (o instanceof BridgingSqlFunction) {
      BridgingSqlFunction bridgingSqlFunction = (BridgingSqlFunction) o;
      return bridgingSqlFunction.getResolvedFunction().getDefinition().getClass();
    } else if (o instanceof BridgingSqlAggFunction) {
      BridgingSqlAggFunction bridgingSqlFunction = (BridgingSqlAggFunction) o;
      return bridgingSqlFunction.getResolvedFunction().getDefinition().getClass();
    }
    return o.getClass();
  }

  public SqlCreateFunction createFunction(String name, String clazz) {
    return new SqlCreateFunction(SqlParserPos.ZERO,
        identifier(name),
        SqlLiteral.createCharString(clazz, SqlParserPos.ZERO),
        "JAVA",
        true,
        true,
        false,
        new SqlNodeList(SqlParserPos.ZERO));
  }

  private Set<SqlCreateTable> extractTableDescriptors(List<WriteQuery> queries) {
    Map<String, ImportedRelationalTable> tables = uniqueSourceExtractor.extract(queries);

    Set<SqlCreateTable> sinksAndSources = new LinkedHashSet<>();
    for (Map.Entry<String, ImportedRelationalTable> table : tables.entrySet()) {
      String tableName = table.getKey();
      SqlCreateTable sqlCreateTable = toCreateTable(tableName, table.getValue().getRowType(),
          table.getValue().getTableSource().getConfiguration(), false);

      sinksAndSources.add(sqlCreateTable);
    }

    return sinksAndSources;
  }

  public SqlCreateTable toCreateTable(String name, RelDataType relDataType, TableConfig tableConfig,
      boolean isSink) {
    TableTableConfig base = tableConfig.getBase();

    return new SqlCreateTable(SqlParserPos.ZERO,
        identifier(name),
        createColumns(relDataType, tableConfig),
        createConstraints(tableConfig.getPrimaryKeyConstraint()),
        createProperties(tableConfig),
        tableConfig.getBase().getPartitionKey().isPresent() ?
            createPartitionKeys(tableConfig.getBase().getPartitionKey().get()) : SqlNodeList.EMPTY,
        base.getWatermarkMillis()>=0 && !isSink ? createWatermark(tableConfig) : null,
        createComment(),
        true,
        false
    );
  }

  private SqlWatermark createWatermark(TableConfig tableConfig) {
    TableTableConfig base = tableConfig.getBase();
    Optional<String> timestampColumn = base.getTimestampColumn()
        .map(f->NamePath.parse(f).getLast().getDisplay());
    if (timestampColumn.isEmpty()) {
      return null;
    }

    long timestampMs = base.getWatermarkMillis();
    return new SqlWatermark(SqlParserPos.ZERO,
        identifier(timestampColumn.get()),
        boundedStrategy(identifier(timestampColumn.get()), Double.toString(timestampMs/1000d)));
  }

  public SqlNodeList createColumns(RelDataType relDataType, TableConfig tableConfig) {
    List<RelDataTypeField> fieldList = relDataType.getFieldList();
    if (fieldList.isEmpty()) {
      return SqlNodeList.EMPTY;
    }
    List<SqlNode> nodes = new ArrayList<>();

    MetadataConfig metadataConfig = tableConfig.getMetadataConfig();

    Map<Name, SqlNode> metadataMap = new HashMap<>();
    for (String columnNameStr : metadataConfig.getKeys()) {
      Name columnName = Name.system(columnNameStr);

      MetadataEntry metadataEntry = metadataConfig.getMetadataEntry(columnNameStr)
          .get();

      Optional<String> attribute = metadataEntry.getAttribute();
      SqlNode node;
      if (attribute.isEmpty()) {
        node = SqlLiteral.createCharString(metadataEntry.getType().get(), SqlParserPos.ZERO);
      } else { //is fnc call
        node = framework.getQueryPlanner().parseCall(attribute.get());
        if (node instanceof SqlIdentifier) {
          node = SqlLiteral.createCharString(metadataEntry.getAttribute().get(), SqlParserPos.ZERO);
        } else {
          SqlCallRewriter callRewriter = new SqlCallRewriter();
          callRewriter.performCallRewrite((SqlCall) node);
        }
      }

      metadataMap.put(columnName, node);
    }

    for (RelDataTypeField column : fieldList) {
      Name columnName = Name.system(column.getName());
      SqlNode node;
      if (metadataMap.containsKey(columnName)) {
        SqlNode metadataFnc = metadataMap.get(columnName);
        if (metadataFnc instanceof SqlCall) {//Is a computed column
          node = new SqlTableColumn.SqlComputedColumn(SqlParserPos.ZERO,
            identifier(column.getKey()), null,
              metadataFnc);
        } else {
          node = new SqlTableColumn.SqlMetadataColumn(SqlParserPos.ZERO,
              identifier(column.getKey()), null,
              SqlDataTypeSpecBuilder.convertTypeToFlinkSpec(column.getType()),
              metadataFnc,
              false);
        }
      } else {
        node = new SqlTableColumn.SqlRegularColumn(SqlParserPos.ZERO,
            identifier(column.getKey()), null,
            SqlDataTypeSpecBuilder.convertTypeToFlinkSpec(column.getType()), null);
      }
      nodes.add(node);
    }

    return new SqlNodeList(nodes, SqlParserPos.ZERO);
  }

  private SqlCreateTable registerSinkTable(WriteSink sink, RelNode relNode) {
    String name;
    TableConfig tableConfig;

    if (sink instanceof EngineSink) {
      EngineSink engineSink = (EngineSink) sink;
      TableConfig engineConfig = engineSink.getStage().getEngine().getSinkConfig(engineSink.getNameId());

      //todo check kafka
      String[] pks = IntStream.of(engineSink.getPrimaryKeys())
          .mapToObj(i -> relNode.getRowType().getFieldList().get(i).getName())
          .toArray(String[]::new);
      tableConfig = engineConfig.toBuilder()
          .setPrimaryKey(pks)
          .build();
      name = engineSink.getNameId();
    } else if (sink instanceof ExternalSink) {
      ExternalSink externalSink = (ExternalSink) sink;
      tableConfig = externalSink.getTableSink().getConfiguration();
      name = externalSink.getName();
    } else {
      throw new RuntimeException("Could not identify write sink type.");
    }

    return toCreateTable(name, relNode.getRowType(), tableConfig, true);
  }

  private List<SqlTableConstraint> createConstraints(List<String> primaryKey) {
    if (primaryKey.isEmpty()) return List.of();

    SqlLiteral pk = SqlUniqueSpec.PRIMARY_KEY.symbol(SqlParserPos.ZERO);
    SqlTableConstraint sqlTableConstraint = new SqlTableConstraint(null, pk,
        new SqlNodeList(primaryKey.stream().map(SqrlToFlinkSqlGenerator::identifier).collect(Collectors.toList()),
            SqlParserPos.ZERO),
        SqlConstraintEnforcement.NOT_ENFORCED.symbol(SqlParserPos.ZERO),
        true,
        SqlParserPos.ZERO);

    return List.of(sqlTableConstraint);
  }


  private SqlNodeList createProperties(TableConfig tableConfig) {
    Map<String, Object> options = new HashMap<>(tableConfig.getConnectorConfig()
        .toMap());
    options.remove("version"); //why are these here?
    options.remove("type");
    if (options.isEmpty()) {
      return SqlNodeList.EMPTY;
    }
    List<SqlNode> props = new ArrayList<>();
    for (Map.Entry<String, Object> option : options.entrySet()) {
      props.add(new SqlTableOption(
          SqlLiteral.createCharString(option.getKey(), SqlParserPos.ZERO),
          SqlLiteral.createCharString(option.getValue().toString(), SqlParserPos.ZERO), //todo: all strings?
          SqlParserPos.ZERO));
    }

    return new SqlNodeList(props, SqlParserPos.ZERO);
  }

  private SqlNodeList createPartitionKeys(List<String> pks) {
    List<SqlIdentifier> partitionKeys = pks
        .stream().map(key -> new SqlIdentifier(key, SqlParserPos.ZERO))
        .collect(Collectors.toList());

    return new SqlNodeList(partitionKeys, SqlParserPos.ZERO);
  }

  private SqlCharStringLiteral createComment() {
    return null;//comment.map(c->SqlLiteral.createCharString(c, SqlParserPos.ZERO))
//        .orElse(null);
  }

  private SqlNode boundedStrategy(SqlNode watermark, String delay) {
    return new SqlBasicCall(
        SqlStdOperatorTable.MINUS,
        new SqlNode[] {
            watermark,
            SqlLiteral.createInterval(
                1,
                delay,
                new SqlIntervalQualifier(
                    TimeUnit.SECOND, TimeUnit.SECOND, SqlParserPos.ZERO),
                SqlParserPos.ZERO)
        },
        SqlParserPos.ZERO);
  }

  private List<WriteQuery> applyFlinkCompatibilityRules(List<? extends Query> queries) {
    List<WriteQuery> collect = queries.stream()
        .map(q -> applyFlinkCompatibilityRules((WriteQuery) q)).collect(Collectors.toList());

    //todo apply downcasting rules
//    RelNode relNode = applyDowncasting(query.getRelNode(), query.getSink(), downcastClassNames);
    return collect;
  }

  private WriteQuery applyFlinkCompatibilityRules(WriteQuery query) {
    RelNode relNode = query.getRelNode();

    Program program = Programs.hep(List.of(PushDownWatermarkHintConfig.DEFAULT.toRule(),
            PushWatermarkHintToTableScanConfig.DEFAULT.toRule(), new ExpandTemporalJoinRule(),
            ExpandWindowHintRuleConfig.DEFAULT.toRule(),
            ShapeBushyCorrelateJoinRuleConfig.DEFAULT.toRule(),
            ExpandNestedTableFunctionRuleConfig.DEFAULT.toRule()

            ), false,
        FlinkDefaultRelMetadataProvider.INSTANCE());

    relNode = program.run(null, relNode, query.getRelNode().getTraitSet(), List.of(), List.of());

    return new WriteQuery(query.getSink(), relNode);
  }

  private void checkPreconditions(List<? extends Query> queries) {
    checkQueriesAreWriteQuery(queries);
  }

  private void checkQueriesAreWriteQuery(List<? extends Query> queries) {
    for (Query query : queries) {
      Preconditions.checkState(query instanceof WriteQuery,
          "Unexpected query type when creating executable plan");
    }
  }

  @lombok.Value
  public static class SqlResult {
    private Set<SqlCreateTable> sinksSources;
    private List<RichSqlInsert> inserts;
    private List<SqlCreateView> queries;
    private List<SqlCreateFunction> functions;
  }
}
