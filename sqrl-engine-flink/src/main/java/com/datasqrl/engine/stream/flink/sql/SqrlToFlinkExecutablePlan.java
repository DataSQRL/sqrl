package com.datasqrl.engine.stream.flink.sql;

import static org.apache.calcite.sql.SqlUtil.stripAs;

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
import com.datasqrl.engine.stream.flink.RowMapperFactory;
import com.datasqrl.engine.stream.flink.sql.calcite.ExpandTemporalJoinRule;
import com.datasqrl.engine.stream.flink.sql.calcite.ExpandWindowHintRule;
import com.datasqrl.engine.stream.flink.sql.calcite.PushDownWatermarkHintRule;
import com.datasqrl.engine.stream.flink.sql.calcite.PushWatermarkHintToTableScanRule;
import com.datasqrl.engine.stream.flink.sql.calcite.ShapeBushyCorrelateJoinRule;
import com.datasqrl.engine.stream.flink.schema.FlinkTypeInfoSchemaGenerator;
import com.datasqrl.engine.stream.flink.schema.UniversalTable2FlinkSchema;
import com.datasqrl.io.DataSystemConnectorConfig;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.calcite.hints.SqrlHint;
import com.datasqrl.plan.calcite.hints.WatermarkHint;
import com.datasqrl.plan.calcite.rel.LogicalStream;
import com.datasqrl.plan.calcite.table.ImportedRelationalTable;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.ExternalSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.Query;
import com.datasqrl.plan.global.PhysicalDAGPlan.WriteQuery;
import com.datasqrl.plan.global.PhysicalDAGPlan.WriteSink;
import com.datasqrl.schema.UniversalTable;
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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
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

  public FlinkBase create(List<? extends Query> queries, Map<String, UserDefinedFunction> udfs,
      Set<URL> jars) {
    checkQueriesAreWriteQuery(queries);

    registerJars(jars);

    List<WriteQuery> writeQueries = applyFlinkCompatibilityRules(queries);
    Map<String, ImportedRelationalTable> tables = extractTablesFromQueries(writeQueries);
    registerFunctions(udfs);

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
            .streamExecutionEnvironmentConfig(Map.of("taskmanager.memory.network.max", "1g"))
            .tableEnvironmentConfig(new HashMap<>())
            //todo: get flink config
            .build())
        .statements(this.statements)
        .functions(this.functions)
        .sinks(this.sinks)
        .tableDefinitions(this.tableDefs)
        .queries(this.queries)
        .errorSink(createErrorSink(errorSink))
        .build();
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
    DataSystemConnectorConfig dsConfig = errorSink.getDsConfig();

    Class factory = FlinkConnectorServiceLoader.resolveSinkClass(dsConfig.getSystemType());

    return FlinkErrorSink.builder()
        .dsConfig(dsConfig)
        .tableConfig(tableConfig)
        .name(errorSink.getName().getDisplay())
        .factory(factory)
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
          Optional.ofNullable(watermarks.getWatermarkExpressions().get(tableName)));
    }
  }

  public void registerSourceTable(String tableName, ImportedRelationalTable relationalTable,
      Optional<SqlNode> watermarkColumn,
      Optional<SqlNode> watermarkExpression) {

    DataSystemConnectorConfig config = relationalTable.getTableSource().getConfiguration()
        .getConnector();
    Class<?> factoryClass = FlinkConnectorServiceLoader.resolveSourceClass(config.getSystemType());

    var type = createTypeInformation(tableName, relationalTable, watermarkColumn,
        watermarkExpression);
    FlinkFactoryDefinition factoryDefinition = FlinkFactoryDefinition.builder()
        .name(tableName)
        .factoryClass(factoryClass)
        .config(config)
        .schemaDefinition(relationalTable.getTableSource().getTableSchema().getDefinition())
        .typeInformation(type.getKey())
        .schema(type.getValue())
        .tableConfig(relationalTable.getTableSource().getConfiguration())
        .build();

    this.tableDefs.add(factoryDefinition);
  }

  private Pair<TypeInformation, Schema> createTypeInformation(String tableName,
      ImportedRelationalTable relationalTable,
      Optional<SqlNode> watermarkColumn, Optional<SqlNode> watermarkExpression) {

    TableSource tableSource = relationalTable.getTableSource();
    //TODO: error handling when mapping doesn't work?
    UniversalTable universalTable = RowMapperFactory.getFlexibleUniversalTableBuilder(
        tableSource.getSchema().getSchema(),
        tableSource.hasSourceTimestamp(), Optional.empty());

    String watermarkName;
    String watermarkExpr;
    boolean isWatermarkColumn;
    if (watermarkExpression.isPresent()) {
      watermarkExpr = RelToFlinkSql.convertToString(watermarkExpression.get());
      watermarkName = removeAllQuotes(watermarkExpr);
      isWatermarkColumn = false;
    } else {
      SqlCall column = (SqlCall) watermarkColumn.get();
      SqlNode name = column.operand(1);
      SqlNode expr = stripAs(column);

      watermarkName = removeAllQuotes(RelToFlinkSql.convertToString(name));
      watermarkExpr = RelToFlinkSql.convertToString(expr);
      isWatermarkColumn = true;
    }

    Schema flinkSchema = convertSchema(universalTable, watermarkName, watermarkExpr,
        isWatermarkColumn);

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

    //Bookkeeping
    final Map<String, SqlNode> watermarkColumns = new HashMap<>();
    final Map<String, SqlNode> watermarkExpressions = new HashMap<>();
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

  private RelNode rewriteIntervals(RelNode relNode) {
    RexBuilder rexBuilder = new RexBuilder(new FlinkTypeFactory(this.getClass().getClassLoader(),
        FlinkTypeSystem.INSTANCE));
    return relNode.accept(
        new RelShuttleImpl() {

          @Override
          public RelNode visit(RelNode other) {
            if (other instanceof LogicalStream) {
              return visit((LogicalStream) other);
            }
            if (other instanceof Snapshot) {
              return visit((Snapshot) other);
            }
            if (other instanceof LogicalTableFunctionScan) {
              return visit((LogicalTableFunctionScan) other);
            }

            return super.visit(other);
          }

          public RelNode visit(Snapshot stream) {
            return this.visitChildren(stream);
          }


          public RelNode visit(LogicalStream stream) {
            return this.visitChildren(stream);
          }

          @Override
          public RelNode visit(TableFunctionScan scan) {
            LogicalTableFunctionScan node = (LogicalTableFunctionScan)
                scan.accept(new RewriteIntervalRexShuttle(rexBuilder));

            return super.visit(node);
          }

          @Override
          public RelNode visit(LogicalProject project) {
            LogicalProject node = (LogicalProject)project.accept(new RewriteIntervalRexShuttle(rexBuilder));

            return super.visit(node);
          }
        }
    );
  }

  @AllArgsConstructor
  public static class RewriteIntervalRexShuttle extends RexShuttle {
    RexBuilder rexBuilder;

    @Override
    public RexWindow visitWindow(RexWindow window) {
      return super.visitWindow(window);
    }

    @Override
      public RexNode visitLiteral(RexLiteral literal) {
        switch (literal.getTypeName().getFamily()) {
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_DAY_TIME:
            final boolean negative = literal.getValueAs(Boolean.class);

            //Override precision for DAY
            SqlIntervalQualifier i =
                new SqlIntervalQualifier(TimeUnit.DAY, 3, null, -1, SqlParserPos.ZERO);

            return rexBuilder.makeIntervalLiteral(
                new BigDecimal(365).multiply(TimeUnit.DAY.multiplier), i);
        }
        return super.visitLiteral(literal);
      }

  }

  private void registerSinkTable(WriteSink sink, RelNode relNode) {
    String connectorName;
    DataSystemConnectorConfig config;
    String name;
    Schema schema;
    TableConfig tableConfig;

    if (sink instanceof EngineSink) {
      EngineSink engineSink = (EngineSink) sink;
      connectorName = engineSink.getStage().getName();
      config = engineSink.getStage().getEngine().getDataSystemConnectorConfig();
      name = engineSink.getNameId();
      schema = new RelNodeToSchemaTransformer()
          .transform(relNode, engineSink.getNumPrimaryKeys());
      tableConfig = null;
    } else if (sink instanceof ExternalSink) {
      ExternalSink externalSink = (ExternalSink) sink;
      connectorName = externalSink.getTableSink().getConnector().getSystemType();
      config = externalSink.getTableSink().getConfiguration().getConnector();
      name = externalSink.getName();
      schema = new RelNodeToSchemaTransformer()
          .transform(relNode, 0);
      tableConfig = externalSink.getTableSink().getConfiguration();
    } else {
      throw new RuntimeException("Could not identify write sink type.");
    }

    TypeInformation typeInformation = new RelNodeToTypeInformationTransformer()
        .transform(relNode);

    Class<?> factory = FlinkConnectorServiceLoader.resolveSinkClass(connectorName);

    FlinkFactoryDefinition factoryDefinition = FlinkFactoryDefinition.builder()
        .factoryClass(factory)
        .config(config)
        .name(name)
        .schema(schema)
        .typeInformation(typeInformation)
        .tableConfig(tableConfig)
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
          addWatermarkExpression(opt.get(), scan.getTable().getRowType(),
              getName(scan.getTable().getQualifiedName()));
        }

        return super.visit(scan);
      }

      private void addWatermarkExpression(WatermarkHint watermarkHint, RelDataType rowType,
          String name) {
        int index = watermarkHint.getTimestampIdx();
        RelDataTypeField field = rowType.getFieldList().get(index);
        watermarks.watermarkExpressions.put(name,
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
            addWatermarkExpression(opt.get(), project.getRowType(), getName(tableName));
          } else {
            Preconditions.checkState(column.getKind() == SqlKind.AS,
                "[Watermark rewriting] Watermark should be aliased");
            Preconditions.checkState(project.getInput() instanceof TableScan,
                "[Watermark rewriting] Watermarks should be above tablescan");
            watermarks.watermarkColumns.put(getName(tableName), column);
          }
        }

        return super.visit(project);
      }
    });
  }

  private Schema convertSchema(UniversalTable universalTable, String watermarkName,
      String watermarkExpression, boolean isWatermarkColumn) {
    Schema.Builder schemaBuilder = Schema.newBuilder();
    List<Pair<String, DataType>> columns = universalTable.convert(new UniversalTable2FlinkSchema());

    for (Pair<String, DataType> column : columns) {
      schemaBuilder.column(column.getKey(), column.getValue());
    }
    if (!isWatermarkColumn) {
      schemaBuilder.watermark(watermarkName, watermarkExpression);
    } else {
      schemaBuilder
          .columnByExpression(watermarkName, watermarkExpression)
          .watermark(watermarkName, "`" + watermarkName + "`");
    }

    return schemaBuilder.build();
  }

  private String removeAllQuotes(String str) {
    return str.replaceAll("`", "");
  }
}
