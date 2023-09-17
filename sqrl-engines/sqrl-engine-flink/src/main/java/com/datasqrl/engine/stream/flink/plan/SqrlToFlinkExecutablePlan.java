package com.datasqrl.engine.stream.flink.plan;

import static org.apache.calcite.sql.SqlUtil.stripAs;

import com.datasqrl.DefaultFunctions;
import com.datasqrl.FlinkExecutablePlan.DefaultFlinkConfig;
import com.datasqrl.FlinkExecutablePlan.FlinkBase;
import com.datasqrl.FlinkExecutablePlan.FlinkErrorSink;
import com.datasqrl.FlinkExecutablePlan.FlinkFactoryDefinition;
import com.datasqrl.FlinkExecutablePlan.FlinkFunction;
import com.datasqrl.FlinkExecutablePlan.FlinkJarStatement;
import com.datasqrl.FlinkExecutablePlan.FlinkJavaFunction;
import com.datasqrl.FlinkExecutablePlan.FlinkQuery;
import com.datasqrl.FlinkExecutablePlan.FlinkSink;
import com.datasqrl.FlinkExecutablePlan.FlinkSqlSink;
import com.datasqrl.FlinkExecutablePlan.FlinkStatement;
import com.datasqrl.FlinkExecutablePlan.FlinkTableDefinition;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.config.DataStreamSourceFactory;
import com.datasqrl.config.FlinkSourceFactory;
import com.datasqrl.config.SinkFactory;
import com.datasqrl.config.SourceFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.TableDescriptorSourceFactory;
import com.datasqrl.engine.stream.flink.sql.*;
import com.datasqrl.engine.stream.flink.sql.rules.ExpandTemporalJoinRule;
import com.datasqrl.engine.stream.flink.sql.rules.ExpandWindowHintRule;
import com.datasqrl.engine.stream.flink.sql.rules.PushDownWatermarkHintRule;
import com.datasqrl.engine.stream.flink.sql.rules.PushWatermarkHintToTableScanRule;
import com.datasqrl.engine.stream.flink.sql.rules.ShapeBushyCorrelateJoinRule;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.ExternalSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.Query;
import com.datasqrl.plan.global.PhysicalDAGPlan.WriteQuery;
import com.datasqrl.plan.global.PhysicalDAGPlan.WriteSink;
import com.datasqrl.plan.hints.SqrlHint;
import com.datasqrl.plan.hints.WatermarkHint;
import com.datasqrl.plan.rel.LogicalStream;
import com.datasqrl.plan.table.ImportedRelationalTable;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.converters.FlinkTypeInfoSchemaGenerator;
import com.datasqrl.schema.converters.SchemaToUniversalTableMapperFactory;
import com.datasqrl.schema.converters.UniversalTable2FlinkSchema;
import com.datasqrl.serializer.SerializableSchema;
import com.datasqrl.serializer.SerializableSchema.WaterMarkType;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.rel2sql.FlinkRelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.plan.metadata.FlinkDefaultRelMetadataProvider;
import org.apache.flink.table.types.DataType;

@Slf4j
@AllArgsConstructor
public class SqrlToFlinkExecutablePlan extends RelShuttleImpl {

  TableSink errorSink;

  private final List<FlinkStatement> statements = new ArrayList<>();
  private final List<FlinkFunction> functions = new ArrayList<>();
  private final List<FlinkSink> sinks = new ArrayList<>();
  private final List<FlinkTableDefinition> tableDefs = new ArrayList<>();
  private final List<FlinkQuery> queries = new ArrayList<>();
  private final FlinkRelToSqlConverter relToSqlConverter = new FlinkRelToSqlConverter(queries);

  public FlinkBase create(SqrlConfig config, List<? extends Query> queries, Map<String, UserDefinedFunction> udfs,
      Set<URL> jars) {
    checkQueriesAreWriteQuery(queries);

    registerJars(jars);

    List<WriteQuery> writeQueries = applyFlinkCompatibilityRules(queries);
    Map<String, ImportedRelationalTable> tables = extractTablesFromQueries(writeQueries);
    //exclude sqrl NOW for flink's NOW
    HashMap<String, UserDefinedFunction> mutableUdfs = new HashMap<>(udfs);
    mutableUdfs.remove(DefaultFunctions.NOW.getName());
    registerFunctions(mutableUdfs);

    WatermarkCollector watermarkCollector = new WatermarkCollector();
    extractWatermarks(writeQueries, watermarkCollector);

    for (WriteQuery query : writeQueries) {
      String tableName = processQuery(query.getRelNode());
      registerSink(tableName, query.getSink().getName());
    }

    registerSourceTables(tables, watermarkCollector);
    registerSinkTables(writeQueries);

    return FlinkBase.builder()
        .config(DefaultFlinkConfig.builder()
            .streamExecutionEnvironmentConfig(getStreamConfig(config))
            .tableEnvironmentConfig(getTableConfig(config))
            .build())
        .statements(this.statements)
        .functions(this.functions)
        .sinks(this.sinks)
        .tableDefinitions(this.tableDefs)
        .queries(this.queries)
        .errorSink(createErrorSink(errorSink))
        .build();
  }

  private Map<String, String> getTableConfig(SqrlConfig config) {
    Map<String, String> conf = new HashMap<>();
    for (Map.Entry<String, String> entry : config.toStringMap().entrySet()) {
      if (entry.getKey().contains(".") && isTableConfigValue(entry.getKey())) {
        conf.put(entry.getKey(), entry.getValue());
      }
    }

    if (!conf.containsKey(ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT.key())) {
      conf.put(ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT.key(), "5000");
    }

    return conf;
  }

  private boolean isTableConfigValue(String key) {
    return key.startsWith("table.") || key.startsWith("sql-client.");
  }

  private Map<String, String> getStreamConfig(SqrlConfig config) {
    Map<String, String> conf = new HashMap<>();
    for (Map.Entry<String, String> entry : config.toStringMap().entrySet()) {
      if (entry.getKey().contains(".") && !isTableConfigValue(entry.getKey())) {
        conf.put(entry.getKey(), entry.getValue());
      }
    }

    return conf;
  }

  private void registerJars(Set<URL> jars) {
    for (URL url : jars) {
      this.statements.add(FlinkJarStatement.builder()
          .path(url.getPath())
          .build());
    }
  }

  private FlinkErrorSink createErrorSink(TableSink errorSink) {
    TableConfig tableConfig = errorSink.getConfiguration();

    Class factory = FlinkConnectorServiceLoader.resolveSinkClass(tableConfig.getConnectorName());

    return FlinkErrorSink.builder()
        .tableConfig(tableConfig.serialize())
        .name(errorSink.getName().getDisplay())
        .connectorFactory(factory)
        .formatFactory(tableConfig.getFormat().getClass())
        .namePath(errorSink.getPath())
        .build();
  }

  private void registerFunctions(Map<String, UserDefinedFunction> udfs) {
    for (Entry<String, UserDefinedFunction> function : udfs.entrySet()) {
      FlinkJavaFunction javaFunction = FlinkJavaFunction.builder()
          .functionName(function.getKey())
          .identifier(function.getValue().getClass().getName())
          .build();
      this.functions.add(javaFunction);
    }
  }

  private List<WriteQuery> applyFlinkCompatibilityRules(List<? extends Query> queries) {
    return queries.stream()
        .map(q -> applyFlinkCompatibilityRules((WriteQuery) q))
        .collect(Collectors.toList());
  }

  private void registerSourceTables(Map<String, ImportedRelationalTable> tables,
      WatermarkCollector watermarks) {
    for (Map.Entry<String, ImportedRelationalTable> table : tables.entrySet()) {
      String tableName = table.getKey();
      registerSourceTable(tableName, table.getValue(),
          Optional.ofNullable(watermarks.getWatermarkColumns().get(tableName)),
          Optional.ofNullable(watermarks.getWatermarkExpression().get(tableName)));
    }
  }

  private Class<? extends FlinkSourceFactory> determineFactory(TableConfig tableConfig) {
    return tableConfig.getFormat().hasSchemaFactory()?
        TableDescriptorSourceFactory.class: DataStreamSourceFactory.class;
  }

  public void registerSourceTable(String tableName, ImportedRelationalTable relationalTable,
      Optional<SqlNode> watermarkColumn,
      Optional<SqlNode> watermarkExpression) {

    TableConfig tableConfig = relationalTable.getTableSource().getConfiguration();
    Class<? extends SourceFactory> connectorFactoryClass = FlinkConnectorServiceLoader.resolveSourceClass(
            tableConfig.getConnectorName(), determineFactory(tableConfig));
    Class<? extends TableSchemaFactory> schemaFactoryClass = tableConfig.getSchemaFactory().map(TableSchemaFactory::getClass).orElse(null);

    Pair<TypeInformation, SerializableSchema> type = createTypeInformation(tableName, relationalTable, watermarkColumn,
        watermarkExpression);
    FlinkFactoryDefinition factoryDefinition = FlinkFactoryDefinition.builder()
        .name(tableName)
        .connectorFactory(connectorFactoryClass)
        .formatFactory(tableConfig.getFormat().getClass())
        .schemaFactory(schemaFactoryClass)
        .schemaDefinition(relationalTable.getTableSource().getSchema().getDefinition())
        .typeInformation(type.getKey())
        .schema(type.getValue())
        .tableConfig(tableConfig.serialize())
        .build();

    this.tableDefs.add(factoryDefinition);
  }

  private Pair<TypeInformation, SerializableSchema> createTypeInformation(String tableName,
      ImportedRelationalTable relationalTable,
      Optional<SqlNode> watermarkColumn, Optional<SqlNode> watermarkExpression) {

    TableSource tableSource = relationalTable.getTableSource();
    //TODO: error handling when mapping doesn't work?
    UniversalTable universalTable = SchemaToUniversalTableMapperFactory.load(tableSource.getSchema())
            .map(tableSource.getSchema(), tableSource.getConnectorSettings(), Optional.empty());

    final String watermarkName;
    final String watermarkExpr;
    final WaterMarkType waterMarkType;
    if (watermarkColumn.isPresent()) { //watermark is a timestamp column
      watermarkExpr = null;
      watermarkName = removeAllQuotes(RelToFlinkSql.convertToString(watermarkColumn.get()));
      if (ReservedName.SOURCE_TIME.getCanonical().equalsIgnoreCase(watermarkName)) {
        waterMarkType = WaterMarkType.SOURCE_WATERMARK;
      } else {
        waterMarkType = WaterMarkType.COLUMN_BY_NAME;
      }
    } else { //watermark is a timestamp expression
      Preconditions.checkArgument(watermarkExpression.isPresent());
      SqlCall call = (SqlCall) watermarkExpression.get();
      SqlNode name = call.operand(1);
      SqlNode expr = stripAs(call);

      watermarkName = removeAllQuotes(RelToFlinkSql.convertToString(name));
      watermarkExpr = RelToFlinkSql.convertToString(expr);

      if (expr instanceof SqlIdentifier && ((SqlIdentifier)expr).getSimple().equalsIgnoreCase(ReservedName.SOURCE_TIME.getCanonical())) {
        waterMarkType = WaterMarkType.SOURCE_WATERMARK;
      } else {
        waterMarkType = WaterMarkType.COLUMN_BY_NAME;
      }
    }

    SerializableSchema flinkSchema = convertSchema(universalTable, watermarkName, watermarkExpr,
        waterMarkType);

    TypeInformation typeInformation = new FlinkTypeInfoSchemaGenerator()
        .convertSchema(universalTable);

    return Pair.of(typeInformation, flinkSchema);
  }

  private void registerSinkTables(List<WriteQuery> writeQueries) {
    for (WriteQuery query : writeQueries) {
      registerSinkTable(query.getSink(), query.getRelNode());
    }
  }

  @Value
  private static class WatermarkCollector {

    //Bookkeeping of watermarks across all tables
    final Map<String, SqlNode> watermarkExpression = new HashMap<>();
    final Map<String, SqlNode> watermarkColumns = new HashMap<>();
  }

  private void checkQueriesAreWriteQuery(List<? extends Query> queries) {
    for (Query query : queries) {
      Preconditions.checkState(query instanceof WriteQuery,
          "Unexpected query type when creating executable plan");
    }
  }

  private void registerSink(String source, String target) {
    this.sinks.add(new FlinkSqlSink(source, target));
  }

  private Map<String, ImportedRelationalTable> extractTablesFromQueries(List<WriteQuery> queries) {
    ExtractUniqueSourceVisitor uniqueSourceExtractor = new ExtractUniqueSourceVisitor();

    for (WriteQuery query : queries) {
      uniqueSourceExtractor.extractFrom(query.getRelNode());
    }

    return uniqueSourceExtractor.getTableMap();
  }

  private WriteQuery applyFlinkCompatibilityRules(WriteQuery query) {
    RelNode relNode = query.getRelNode();

    Program program = Programs.hep(
        List.of(
            PushDownWatermarkHintRule.Config.DEFAULT.toRule(),
            PushWatermarkHintToTableScanRule.Config.DEFAULT.toRule(),
            new ExpandTemporalJoinRule(),
            ExpandWindowHintRule.Config.DEFAULT.toRule(),
            ShapeBushyCorrelateJoinRule.Config.DEFAULT.toRule()
        ),
        false, FlinkDefaultRelMetadataProvider.INSTANCE());


    relNode = program.run(null, relNode,
        query.getRelNode().getTraitSet(), List.of(), List.of());

    return new WriteQuery(query.getSink(), relNode);
  }

  private void registerSinkTable(WriteSink sink, RelNode relNode) {
    String name;
    SerializableSchema schema;
    TableConfig tableConfig;

    if (sink instanceof EngineSink) {
      EngineSink engineSink = (EngineSink) sink;
      tableConfig = engineSink.getStage().getEngine().getSinkConfig(engineSink.getNameId());
      name = engineSink.getNameId();
      schema = new RelNodeToSchemaTransformer()
          .transform(relNode, engineSink.getNumPrimaryKeys());
    } else if (sink instanceof ExternalSink) {
      ExternalSink externalSink = (ExternalSink) sink;
      tableConfig = externalSink.getTableSink().getConfiguration();
      name = externalSink.getName();
      schema = new RelNodeToSchemaTransformer()
          .transform(relNode, 0);
    } else {
      throw new RuntimeException("Could not identify write sink type.");
    }

    String connectorName = tableConfig.getConnectorName();
    TypeInformation typeInformation = new RelNodeToTypeInformationTransformer()
        .transform(relNode);

    Class<? extends SinkFactory> factory = FlinkConnectorServiceLoader.resolveSinkClass(connectorName);
    Class<? extends TableSchemaFactory> schemaFactoryClass = tableConfig.getSchemaFactory().map(TableSchemaFactory::getClass).orElse(null);

    FlinkFactoryDefinition factoryDefinition = FlinkFactoryDefinition.builder()
        .connectorFactory(factory)
        .formatFactory(tableConfig.getFormat().getClass())
        .name(name)
        .schemaFactory(schemaFactoryClass)
        .schema(schema)
        .typeInformation(typeInformation)
        .tableConfig(tableConfig.serialize())
        .build();

    this.tableDefs.add(factoryDefinition);
  }

  private String processQuery(RelNode relNode) {
    return RelToFlinkSql.convertToSql(relToSqlConverter, relNode);
  }

  private void extractWatermarks(List<WriteQuery> writeQueries,
      WatermarkCollector watermarkCollector) {
    for (WriteQuery q : writeQueries) {
      extractWatermarks(q.getRelNode(), watermarkCollector);
    }
  }

  private void extractWatermarks(RelNode relNode, WatermarkCollector watermarks) {
    relNode.accept(new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        Optional<WatermarkHint> opt = SqrlHint.fromRel(scan, WatermarkHint.CONSTRUCTOR);
        if (opt.isPresent()) {
          addWatermarkColumn(opt.get(), scan.getTable().getRowType(),
              getName(scan.getTable().getQualifiedName()));
        }

        return super.visit(scan);
      }

      private void addWatermarkColumn(WatermarkHint watermarkHint, RelDataType rowType,
          String tableName) {
        int index = watermarkHint.getTimestampIdx();
        RelDataTypeField field = rowType.getFieldList().get(index);
        watermarks.watermarkColumns.put(tableName,
            new SqlIdentifier(field.getName(), SqlParserPos.ZERO));
      }

      private String getName(List<String> qualifiedName) {
        return qualifiedName.get(qualifiedName.size() - 1);
      }

      @Override
      public RelNode visit(LogicalProject project) {
        Optional<WatermarkHint> opt = SqrlHint.fromRel(project, WatermarkHint.CONSTRUCTOR);
        if (opt.isPresent()) {
          SqlSelect select = (SqlSelect) RelToFlinkSql.convertToSqlNode(project);
          int index = opt.get().getTimestampIdx();
          SqlNode column = select.getSelectList().get(index);
          List<String> tableName = project.getInput().getTable().getQualifiedName();
          if (column.getKind() != SqlKind.AS) {
            addWatermarkColumn(opt.get(), project.getRowType(), getName(tableName));
          } else {
            Preconditions.checkState(column.getKind() == SqlKind.AS,
                "[Watermark rewriting] Watermark should be aliased");
            Preconditions.checkState(project.getInput() instanceof TableScan,
                "[Watermark rewriting] Watermarks should be above tablescan");
            watermarks.watermarkExpression.put(getName(tableName), column);
          }
        }

        return super.visit(project);
      }
    });
  }

  private SerializableSchema convertSchema(UniversalTable universalTable, String watermarkName,
      String watermarkExpression, WaterMarkType waterMarkType) {
    List<Pair<String, DataType>> columns = universalTable.convert(new UniversalTable2FlinkSchema());

    return SerializableSchema.builder()
        .columns(columns)
        .waterMarkType(waterMarkType)
        .watermarkName(watermarkName)
        .watermarkExpression(watermarkExpression)
        .build();
  }


  private String removeAllQuotes(String str) {
    return str.replaceAll("`", "");
  }
}
