package com.datasqrl.sql;


import static com.datasqrl.FlinkEnvironmentBuilder.UUID_FCT_NAME;
import static com.datasqrl.FlinkEnvironmentBuilder.getTableDescriptor;
import static com.datasqrl.FlinkEnvironmentBuilder.toSchema;
import static com.datasqrl.engine.stream.flink.plan.SqrlToFlinkExecutablePlan.removeAllQuotes;
import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;
import static org.apache.calcite.sql.SqlUtil.stripAs;

import com.datasqrl.FlinkEnvironmentBuilder;
import com.datasqrl.FlinkExecutablePlan;
import com.datasqrl.FlinkExecutablePlan.DefaultFlinkConfig;
import com.datasqrl.FlinkExecutablePlan.FlinkBase;
import com.datasqrl.FlinkExecutablePlan.FlinkBaseVisitor;
import com.datasqrl.FlinkExecutablePlan.FlinkConfigVisitor;
import com.datasqrl.FlinkExecutablePlan.FlinkExecutablePlanVisitor;
import com.datasqrl.FlinkExecutablePlan.FlinkFactoryDefinition;
import com.datasqrl.FlinkExecutablePlan.FlinkFunctionVisitor;
import com.datasqrl.FlinkExecutablePlan.FlinkJarStatement;
import com.datasqrl.FlinkExecutablePlan.FlinkJavaFunction;
import com.datasqrl.FlinkExecutablePlan.FlinkQueryVisitor;
import com.datasqrl.FlinkExecutablePlan.FlinkSinkVisitor;
import com.datasqrl.FlinkExecutablePlan.FlinkSqlFunction;
import com.datasqrl.FlinkExecutablePlan.FlinkSqlQuery;
import com.datasqrl.FlinkExecutablePlan.FlinkSqlSink;
import com.datasqrl.FlinkExecutablePlan.FlinkSqlTableApiDefinition;
import com.datasqrl.FlinkExecutablePlan.FlinkStatementVisitor;
import com.datasqrl.FlinkExecutablePlan.FlinkStreamQuery;
import com.datasqrl.FlinkExecutablePlan.FlinkTableDefinitionVisitor;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;
import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.config.BaseConnectorFactory;
import com.datasqrl.config.DataStreamSourceFactory;
import com.datasqrl.config.FlinkSinkFactoryContext;
import com.datasqrl.config.FlinkSourceFactoryContext;
import com.datasqrl.config.SinkFactory;
import com.datasqrl.config.TableDescriptorSourceFactory;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.serializer.SerializableSchema.WaterMarkType;
import com.datasqrl.sql.FlinkSqlGenerator.FlinkSqlContext;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCollectionTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.ddl.SqlAddJar;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlConstraintEnforcement;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.constraint.SqlUniqueSpec;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.dml.SqlExecute;
import org.apache.flink.sql.parser.dml.SqlStatementSet;
import org.apache.flink.sql.parser.type.ExtendedSqlCollectionTypeNameSpec;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;

/**
 *
 */
public class FlinkSqlGenerator implements
    FlinkExecutablePlanVisitor<List<SqlNode>, FlinkSqlContext>,
    FlinkBaseVisitor<List<SqlNode>, FlinkSqlContext>,
    FlinkConfigVisitor<List<SqlSet>, FlinkSqlContext>,
    FlinkStatementVisitor<SqlAddJar, FlinkSqlContext>,
    FlinkFunctionVisitor<SqlCreateFunction, FlinkSqlContext>,
    FlinkTableDefinitionVisitor<SqlCreateTable, FlinkSqlContext>,
    FlinkQueryVisitor<SqlCreateView, FlinkSqlContext>,
    FlinkSinkVisitor<RichSqlInsert, FlinkSqlContext> {


  @Override
  public List<SqlNode> visitPlan(FlinkExecutablePlan plan, FlinkSqlContext context) {
    return plan.getBase().accept(this, context);
  }

  @Override
  public List<SqlNode> visitBase(FlinkBase base, FlinkSqlContext context) {
    // SET 'execution.runtime-mode' = 'stream';
    List<SqlSet> config = base.getConfig().accept(this, context);

    List<SqlAddJar> jars = base.getStatements().stream().map(t -> t.accept(this, context))
        .collect(Collectors.toList());

    List<SqlCreateFunction> functions = base.getFunctions().stream()
        .map(t -> t.accept(this, context)).collect(Collectors.toList());

    List<SqlCreateTable> sources = base.getTableDefinitions().stream()
        .map(t -> t.accept(this, context)).collect(Collectors.toList());

    List<SqlCreateView> queries = base.getQueries().stream().map(t -> t.accept(this, context))
        .collect(Collectors.toList());

    List<RichSqlInsert> sinks = base.getSinks().stream().map(t -> t.accept(this, context))
        .collect(Collectors.toList());

    SqlStatementSet sqlStatementSet = new SqlStatementSet(sinks, SqlParserPos.ZERO);

    SqlExecute execute = new SqlExecute(sqlStatementSet, SqlParserPos.ZERO);
    Iterable<SqlNode> combinedIterable = Iterables.concat(config, jars, functions, sources, queries,
        List.of(execute));

    return Lists.newArrayList(combinedIterable);
  }

  @Override
  public List<SqlSet> visitConfig(DefaultFlinkConfig config, FlinkSqlContext context) {
    return Streams
        .concat(config.getTableEnvironmentConfig().entrySet().stream(),
            config.getStreamExecutionEnvironmentConfig().entrySet().stream())
        .map(entry -> new SqlSet(SqlParserPos.ZERO, SqlLiteral.createCharString(entry.getKey(), SqlParserPos.ZERO),
            SqlLiteral.createCharString(entry.getValue(), SqlParserPos.ZERO)))
        .collect(Collectors.toList());
  }

  @Override
  public SqlAddJar visitJarStatement(FlinkJarStatement statement, FlinkSqlContext context) {
    return new SqlAddJar(SqlParserPos.ZERO, SqlLiteral.createCharString(statement.getPath(), SqlParserPos.ZERO));
  }

  @Override
  public SqlCreateFunction visitFunction(FlinkJavaFunction fnc, FlinkSqlContext context) {
    return new SqlCreateFunction(SqlParserPos.ZERO,
        identifier(fnc.getFunctionName()),
        SqlLiteral.createCharString(fnc.getIdentifier(), SqlParserPos.ZERO),
        "JAVA",
        true,
        true,
        false,
        new SqlNodeList(SqlParserPos.ZERO));
  }

  @Override
  public SqlCreateFunction visitFunction(FlinkSqlFunction flinkSqlFunction,
      FlinkSqlContext context) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public SqlCreateTable visitTableDefinition(FlinkSqlTableApiDefinition table,
      FlinkSqlContext context) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public SqlCreateTable visitFactoryDefinition(FlinkFactoryDefinition table,
      FlinkSqlContext context) {
    String name = table.getName();
    TableConfig tableConfig = table.getTableConfig().deserialize(ErrorCollector.root());

    FormatFactory formatFactory = FlinkEnvironmentBuilder.createFactoryInstance(table.getFormatFactory());
    BaseConnectorFactory connectorFactory = FlinkEnvironmentBuilder.createFactoryInstance(table.getConnectorFactory());
    if (connectorFactory instanceof DataStreamSourceFactory) {
      throw new RuntimeException("Not supported");
    } else if (connectorFactory instanceof TableDescriptorSourceFactory) {
      TableDescriptorSourceFactory sourceFactory = (TableDescriptorSourceFactory) connectorFactory;
      FlinkSourceFactoryContext factoryContext = new FlinkSourceFactoryContext(null, name, tableConfig.serialize(),
          formatFactory, UUID.randomUUID());
      TableDescriptor descriptor = getTableDescriptor(sourceFactory, factoryContext, table.getSchema());

      return toCreateTable(name, descriptor,(RelDataType) table.getRelDataType(),
          (Optional<SqlNode>)table.getWatermarkExpression(),
          (Optional<SqlNode>)table.getWatermarkColumn(),
          sourceFactory.getSourceTimeMetaData(), true,
          table.getSchema().getPrimaryKey()
          );
    } else if (connectorFactory instanceof SinkFactory) {
      SinkFactory sinkFactory = (SinkFactory) connectorFactory;
      Object o = sinkFactory.create(
          new FlinkSinkFactoryContext(name, tableConfig, formatFactory));
      if (o instanceof DataStream) {
        throw new RuntimeException("Not supported");
      } else if (o instanceof TableDescriptor.Builder) {
        TableDescriptor.Builder builder = (TableDescriptor.Builder) o;
        TableDescriptor descriptor = builder.schema(toSchema(table.getSchema()))
            .build();

        return toCreateTable(name, descriptor, (RelDataType) table.getRelDataType(),
            (Optional<SqlNode>) table.getWatermarkExpression(),
            (Optional<SqlNode>) table.getWatermarkColumn(), Optional.empty(),
            false, table.getSchema().getPrimaryKey());
      } else {
        throw new RuntimeException("Unknown sink type");
      }

    } else {
      throw new RuntimeException("Unknown factory type " + connectorFactory.getClass().getName());
    }
  }

  private SqlCreateTable toCreateTable(String name, TableDescriptor descriptor, RelDataType relDataType,
      Optional<SqlNode> watermarkExpression, Optional<SqlNode> watermarkColumn,
      Optional<String> sourceTimeMetaData, boolean setMetadata, List<String> primaryKey) {
    return new SqlCreateTable(SqlParserPos.ZERO,
        identifier(name),
        createColumns(descriptor.getSchema(), relDataType, watermarkExpression, watermarkColumn, sourceTimeMetaData, setMetadata), //columns
        createConstraints(descriptor.getSchema(), primaryKey), //table constraints
        createProperties(descriptor.getOptions()), //property list ?
        createPartitionKeys(descriptor.getPartitionKeys()), //partition key list
        createWatermark(descriptor.getSchema(),watermarkColumn, watermarkExpression, setMetadata),
        createComment(descriptor.getComment()),
        true,
        false
    );
  }

  private SqlWatermark createWatermark(Optional<Schema> schema,
      Optional<SqlNode> watermarkColumn, Optional<SqlNode> watermarkExpression,
      boolean setMetadata) {
    if (!setMetadata) {
      return null;
    }
    final WaterMarkType waterMarkType;
    if (watermarkColumn.isPresent()) { //watermark is a timestamp column
      waterMarkType = WaterMarkType.COLUMN_BY_NAME;
    } else { //watermark is a timestamp expression
      Preconditions.checkArgument(watermarkExpression.isPresent());
      waterMarkType = WaterMarkType.COLUMN_BY_NAME;
    }

    String name = getWatermarkName(watermarkColumn, watermarkExpression);

    com.google.common.base.Preconditions.checkArgument(
        setMetadata || waterMarkType == WaterMarkType.COLUMN_BY_NAME);
    return new SqlWatermark(SqlParserPos.ZERO,
        identifier(name),
        boundedStrategy(name, "1"));
  }

  private String getWatermarkName(Optional<SqlNode> watermarkColumn,
      Optional<SqlNode> watermarkExpression) {
    if (watermarkColumn.isPresent()) { //watermark is a timestamp column
      return removeAllQuotes(RelToFlinkSql.convertToString(watermarkColumn.get()));
    } else { //watermark is a timestamp expression
      Preconditions.checkArgument(watermarkExpression.isPresent());
      SqlCall call = (SqlCall) watermarkExpression.get();
      SqlNode name = call.operand(1);
      return removeAllQuotes(RelToFlinkSql.convertToString(name));
    }
  }

  private SqlNodeList createColumns(Optional<Schema> schemaOpt, RelDataType relDataType,
      Optional<SqlNode> watermarkExpression, Optional<SqlNode> watermarkColumn,
      Optional<String> sourceTimeMetaData, boolean setMetadata) {
    if (relDataType.getFieldList().isEmpty()) {
      return SqlNodeList.EMPTY;
    }
    return createColumns(relDataType.getFieldList(),
        watermarkColumn, watermarkExpression, setMetadata, sourceTimeMetaData);

  }

  public SqlNodeList createColumns(List<RelDataTypeField> fields,
      Optional<SqlNode> watermarkColumn,
      Optional<SqlNode> watermarkExpression,
      boolean setMetadata,
      Optional<String> sourceTime
  ) {

    List<SqlNode> nodes = new ArrayList<>();

    //TODO: This is brittle, it mirrors the structure of UniversalTable by index
    int index = 0;
    for (RelDataTypeField column : fields) {
      SqlNode node;
      if (setMetadata && index==0) {
        node = new SqlTableColumn.SqlComputedColumn(SqlParserPos.ZERO,
            identifier(column.getKey()), null,
            lightweightOp(UUID_FCT_NAME).createCall(SqlParserPos.ZERO));
      } else if (setMetadata && index==1) {
        node = new SqlTableColumn.SqlComputedColumn(SqlParserPos.ZERO,
            identifier(column.getKey()), null,
            lightweightOp("PROCTIME").createCall(SqlParserPos.ZERO));
      } else if (setMetadata && index==2 && sourceTime.isPresent()) {
        node = new SqlTableColumn.SqlMetadataColumn(SqlParserPos.ZERO,
            identifier(column.getKey()), null,
            SqlDataTypeSpecBuilder.convertTypeToFlinkSpec(column.getType()),
            SqlLiteral.createCharString(sourceTime.get(),SqlParserPos.ZERO), //todo: should be expression? Pass in a sqlNode (should be parsed upstream)
            false);
      } else {
        node = new SqlTableColumn.SqlRegularColumn(SqlParserPos.ZERO,
            identifier(column.getKey()), null,
            (SqlDataTypeSpecBuilder.convertTypeToFlinkSpec(column.getType())), null);
      }

      nodes.add(node);

      index++;
    }

    if (watermarkExpression.isPresent()) {
      nodes.add(new SqlTableColumn.SqlComputedColumn(SqlParserPos.ZERO,
          identifier(getWatermarkName(watermarkColumn, watermarkExpression)),
          null,
          watermarkExpression.get()));
    }

    return new SqlNodeList(nodes, SqlParserPos.ZERO);
  }

  private List<SqlTableConstraint> createConstraints(Optional<Schema> schema,
      List<String> primaryKey) {
    if (primaryKey == null || primaryKey.isEmpty()) return List.of();
    SqlLiteral pk = SqlUniqueSpec.PRIMARY_KEY.symbol(SqlParserPos.ZERO);
    SqlTableConstraint sqlTableConstraint = new SqlTableConstraint(null, pk,
        new SqlNodeList(primaryKey.stream().map(p -> identifier(p)).collect(Collectors.toList()),
            SqlParserPos.ZERO),
        SqlConstraintEnforcement.NOT_ENFORCED.symbol(SqlParserPos.ZERO),
        true,
        SqlParserPos.ZERO);

    return List.of(sqlTableConstraint);
  }

  private SqlNodeList createProperties(Map<String, String> options) {
    if (options.isEmpty()) {
      return SqlNodeList.EMPTY;
    }
    List<SqlNode> props = new ArrayList<>();
    for (Map.Entry<String, String> option : options.entrySet()) {
      props.add(new SqlTableOption(
          SqlLiteral.createCharString(option.getKey(), SqlParserPos.ZERO),
          SqlLiteral.createCharString(option.getValue(), SqlParserPos.ZERO),
          SqlParserPos.ZERO));
    }

    return new SqlNodeList(props, SqlParserPos.ZERO);
  }

  private SqlNodeList createPartitionKeys(List<String> partitionKeys) {
    return new SqlNodeList(partitionKeys.stream()
        .map(key -> new SqlIdentifier(key, SqlParserPos.ZERO))
        .collect(Collectors.toList()), SqlParserPos.ZERO);
  }

  private SqlCharStringLiteral createComment(Optional<String> comment) {
    return comment.map(c->SqlLiteral.createCharString(c, SqlParserPos.ZERO))
        .orElse(null);
  }

  private SqlNode boundedStrategy(String rowtimeColumn, String delay) {
    return new SqlBasicCall(
        SqlStdOperatorTable.MINUS,
        new SqlNode[] {
            identifier(rowtimeColumn),
            SqlLiteral.createInterval(
                1,
                delay,
                new SqlIntervalQualifier(
                    TimeUnit.SECOND, TimeUnit.SECOND, SqlParserPos.ZERO),
                SqlParserPos.ZERO)
        },
        SqlParserPos.ZERO);
  }
  @Override
  public SqlCreateView visitQuery(FlinkSqlQuery query, FlinkSqlContext context) {
    return new SqlCreateView(SqlParserPos.ZERO,
        identifier(query.getName()),
        SqlNodeList.EMPTY,
        (SqlNode) query.getNode(),
        false,
        false,
        false,
        null,
        null
    );
  }

  @Override
  public SqlCreateView visitQuery(FlinkStreamQuery query, FlinkSqlContext context) {
    throw new RuntimeException("Not supported");

  }

  @Override
  public RichSqlInsert visitSink(FlinkSqlSink table, FlinkSqlContext context) {
    return new RichSqlInsert(SqlParserPos.ZERO,
        SqlNodeList.EMPTY,
        SqlNodeList.EMPTY,
        identifier(table.getTarget()),
        createSelect(table.getSource()),
        null,
        null
    );
  }

  private SqlNode createSelect(String source) {
    return new SqlSelectBuilder()
        .setFrom(identifier(source))
        .build();
  }

  public static class FlinkSqlContext {

  }

  public SqlIdentifier identifier(String str) {
    return new SqlIdentifier(str, SqlParserPos.ZERO);
  }
}
