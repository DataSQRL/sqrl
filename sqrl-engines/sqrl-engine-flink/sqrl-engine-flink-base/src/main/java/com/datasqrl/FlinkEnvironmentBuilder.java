package com.datasqrl;

import com.datasqrl.FlinkExecutablePlan.DefaultFlinkConfig;
import com.datasqrl.FlinkExecutablePlan.FlinkBase;
import com.datasqrl.FlinkExecutablePlan.FlinkBaseVisitor;
import com.datasqrl.FlinkExecutablePlan.FlinkConfigVisitor;
import com.datasqrl.FlinkExecutablePlan.FlinkDataStreamDefinition;
import com.datasqrl.FlinkExecutablePlan.FlinkErrorSink;
import com.datasqrl.FlinkExecutablePlan.FlinkErrorSinkVisitor;
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
import com.datasqrl.FlinkEnvironmentBuilder.PlanContext;
import com.datasqrl.config.BaseConnectorFactory;
import com.datasqrl.config.SinkFactory;
import com.datasqrl.config.SinkFactory.FlinkSinkFactoryContext;
import com.datasqrl.config.SourceFactory;
import com.datasqrl.config.TableDescriptorSinkFactory;
import com.datasqrl.engine.stream.FunctionWithError;
import com.datasqrl.engine.stream.flink.InputError;
import com.datasqrl.engine.stream.flink.InputError.InputErrorMessage;
import com.datasqrl.engine.stream.flink.InputError.Map2InputErrorMessage;
import com.datasqrl.engine.stream.flink.MapWithErrorProcess;
import com.datasqrl.engine.stream.flink.schema.FlinkRowConstructor;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.DataSystemConnectorConfig;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.SourceRecord.Named;
import com.datasqrl.io.SourceRecord.Raw;
import com.datasqrl.io.formats.Format;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.model.schema.SchemaDefinition;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.datasqrl.model.LogicalStreamMetaData;
import com.datasqrl.StreamTableConverter.ConvertToStream;
import com.datasqrl.StreamTableConverter.EmitFirstInsertOrUpdate;
import com.datasqrl.StreamTableConverter.KeyedIndexSelector;
import com.datasqrl.StreamTableConverter.RowMapper;
import com.datasqrl.schema.converters.FlexibleSchemaRowMapper;
import com.datasqrl.schema.input.DefaultSchemaValidator;
import com.datasqrl.schema.input.FlexibleTableSchemaFactory;
import com.datasqrl.schema.input.SchemaValidator;
import com.datasqrl.model.StreamType;
import com.datasqrl.FlinkFormatFactory.FlinkTextLineFormat;
import com.datasqrl.serializer.SerializableSchema;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

@Slf4j
public class FlinkEnvironmentBuilder implements
    FlinkExecutablePlanVisitor<StatementSet, Object>,
    FlinkBaseVisitor<StatementSet, Object>,
    FlinkConfigVisitor<PlanContext, Object>,
    FlinkFunctionVisitor<Object, PlanContext>,
    FlinkTableDefinitionVisitor<Object, PlanContext>,
    FlinkQueryVisitor<Object, PlanContext>,
    FlinkSinkVisitor<Object, PlanContext>,
    FlinkErrorSinkVisitor<Object, PlanContext>,
    FlinkStatementVisitor<Object, PlanContext> {

  public static final String ERROR_TAG_PREFIX = "_errors";
  public static final String ERROR_SINK_NAME = "errors_internal_sink";

  @Override
  public StatementSet visitPlan(FlinkExecutablePlan plan, Object context) {
    return plan.getBase().accept(this, null);
  }

  @Override
  public StatementSet visitBase(FlinkBase base, Object context) {
    PlanContext planCtx = base.getConfig().accept(this, context);
    base.getStatements().stream()
        .forEach(f -> f.accept(this, planCtx));
    base.getFunctions().stream()
        .forEach(f -> f.accept(this, planCtx));
    base.getTableDefinitions().stream()
        .forEach(f -> f.accept(this, planCtx));
    base.getQueries().stream()
        .forEach(f -> f.accept(this, planCtx));
    base.getSinks().stream()
        .forEach(f -> f.accept(this, planCtx));

    if (base.getErrorSink() != null) {
      base.getErrorSink().accept(this, planCtx);
    }

    return planCtx.getStatementSet();
  }

  private void registerErrors(DataStream<InputError> errorStream, FlinkErrorSink sink,
      PlanContext context) {

    DataStream<InputErrorMessage> errorMessages = errorStream.flatMap(new Map2InputErrorMessage());
    Schema errorTableSchema = InputError.InputErrorMessage.getTableSchema();
    Table errorTable = context.getTEnv().fromDataStream(errorMessages, errorTableSchema);

    TableDescriptorSinkFactory builderClass = (TableDescriptorSinkFactory) createFactoryInstance(
        sink.getFactory());
    TableDescriptor descriptor = builderClass.create(new FlinkSinkFactoryContext(sink.getName(),
            sink.getDsConfig(), sink.getTableConfig(), context.getErrors()))
        .schema(errorTableSchema)
        .build();

    context.getTEnv().createTemporaryTable(ERROR_SINK_NAME, descriptor);

    context.getStatementSet().addInsert(ERROR_SINK_NAME, errorTable);
  }

  private Optional<DataStream<InputError>> createErrorStream(PlanContext planCtx) {
    List<SideOutputDataStream> errorStreams = planCtx.getErrorStreams();
    if (errorStreams.size() > 0) {
      DataStream<InputError> combinedStream = errorStreams.get(0);
      if (errorStreams.size() > 1) {
        combinedStream = combinedStream.union(
            errorStreams.subList(1, errorStreams.size()).toArray(size -> new DataStream[size]));
      }
      return Optional.of(combinedStream);
    } else {
      return Optional.empty();
    }
  }

  @Override
  public PlanContext visitConfig(DefaultFlinkConfig config, Object context) {
    log.debug("Setting flink config");
    Configuration sEnvConfig = Configuration.fromMap(
        config.getStreamExecutionEnvironmentConfig());
    StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment(
        sEnvConfig);

    EnvironmentSettings tEnvConfig = EnvironmentSettings.newInstance()
        .withConfiguration(Configuration.fromMap(config.getTableEnvironmentConfig())).build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);

    return new PlanContext(sEnv, tEnv, tEnv.createStatementSet());
  }

  @Override
  public Object visitFunction(FlinkJavaFunction fnc, PlanContext context) {
    log.debug("Creating function {}", fnc.getFunctionName());

    String sql = createFunctionStatement(fnc);
    context.getTEnv()
        .executeSql(sql);
    return null;
  }

  @Override
  public Object visitFunction(FlinkSqlFunction fnc, PlanContext context) {
    log.debug("Creating function sql {}", fnc.getFunctionSql());
    context.getTEnv()
        .executeSql(fnc.getFunctionSql());
    return null;
  }

  /**
   * CREATE [TEMPORARY|TEMPORARY SYSTEM] FUNCTION [IF NOT EXISTS]
   * [catalog_name.][db_name.]function_name AS identifier [LANGUAGE JAVA|SCALA|PYTHON] [USING JAR
   * '<path_to_filename>.jar' [, JAR '<path_to_filename>.jar']* ]
   */
  public String createFunctionStatement(FlinkJavaFunction fnc) {
//    String temporary = fnc.isSystemFunction() ? "TEMPORARY SYSTEM" : "TEMPORARY";
//    String ifNotExistsClause = fnc.isIfNotExists() ? "IF NOT EXISTS" : "";
//    String catalogDb = (fnc.getCatalogName() != null ? fnc.getCatalogName() + "." : "") +
//        (fnc.getDbName() != null ? fnc.getDbName() + "." : "");
//    String language =
//        "LANGUAGE " + (fnc.getLang() != null ? fnc.getLang().toUpperCase() : "JAVA");
//    String jars =
//        fnc.getJarPaths() != null && !fnc.getJarPaths().isEmpty() ? fnc.getJarPaths().stream()
//            .map(jarPath -> String.format("JAR '%s'", jarPath))
//            .collect(Collectors.joining(", ")) : "";

    return String.format("CREATE %s FUNCTION %s %s AS '%s' LANGUAGE %s",
        "TEMPORARY",
        "IF NOT EXISTS",
        fnc.getFunctionName(),
        fnc.getIdentifier(),
        "JAVA");
  }

  @Override
  public Object visitSink(FlinkSqlSink table, PlanContext context) {
    log.debug("Creating sink {} -> {}", table.getSource(), table.getTarget());
    context.getStatementSet()
        .addInsert(table.getTarget(),
            context.getTEnv().from(table.getSource()));
    return null;
  }

  @Override
  public Object visitQuery(FlinkSqlQuery query, PlanContext context) {
    log.debug("Creating SQL table: {} {}", query.getName(), query.getQuery());
    Table table = context.getTEnv().sqlQuery(query.getQuery());
    context.getTEnv().createTemporaryView(query.getName(), table);
    return null;
  }

  @Override
  public Object visitQuery(FlinkStreamQuery query, PlanContext context) {
    StreamType streamType = query.getStateChangeType();
    LogicalStreamMetaData baseRelationMeta = query.getMeta();
    boolean unmodifiedChangelog = query.isUnmodifiedChangelog();
    log.debug("Creating Stream query {}, {}", query.getName(), query.getFromTable());
    Table table = context.getTEnv().from(query.getFromTable());

    DataStream<Row> stream = context.getTEnv()
        .toChangelogStream(table, table.getSchema().toSchema(), ChangelogMode.upsert());
//        .process(new Inspector("Raw data"));

    switch (streamType) {
      case ADD:
        if (unmodifiedChangelog) {
          //We can simply filter on RowKind
          stream = stream.filter(row -> row.getKind()==RowKind.INSERT);
        } else {
          //Only emit the first insert or update per key
          stream = stream.keyBy(new KeyedIndexSelector(baseRelationMeta.getKeyIdx()))
              .flatMap(new EmitFirstInsertOrUpdate());
        }
        break;
      case DELETE:
        Preconditions.checkArgument(unmodifiedChangelog,"Cannot create DELETE stream from modified state table. Invert filter and use ADD instead.");
        stream = stream.filter(row -> row.getKind()==RowKind.DELETE);
        break;
      case UPDATE:
        stream = stream.filter(row -> row.getKind()==RowKind.INSERT || row.getKind()==RowKind.UPDATE_AFTER);
        break;
      default:
        throw new UnsupportedOperationException("Unexpected state change type: " + streamType);
    }

//      RowTypeInfo typeInfo = new RowTypeInfo(table.getSchema().getFieldTypes(),
//      table.getSchema().getFieldNames());

    //Adds rows
    RowMapper rowMapper = new RowMapper(baseRelationMeta);
    stream = stream.process(new ConvertToStream(rowMapper), query.getTypeInformation());

    //register
    context.getTEnv().createTemporaryView(query.getName(), stream, toSchema(query.getSchema()));
    return null;
  }

  private Schema toSchema(SerializableSchema schema) {
    Schema.Builder builder = Schema.newBuilder();

    for (Pair<String, DataType> column : schema.getColumns()) {
      builder.column(column.getKey(), column.getValue());
    }

    if (schema.getIsWatermarkColumn() != null) {
      if (!schema.getIsWatermarkColumn()) {
        builder.watermark(schema.getWatermarkName(), schema.getWatermarkExpression());
      } else {
        builder
            .columnByExpression(schema.getWatermarkName(), schema.getWatermarkExpression())
            .watermark(schema.getWatermarkName(), "`" + schema.getWatermarkName() + "`");
      }
    }
    if (schema.getPrimaryKey() != null && !schema.getPrimaryKey().isEmpty()) {
      builder.primaryKey(schema.getPrimaryKey());
    }
    return builder.build();
  }

  @Override
  public Object visitErrorSink(FlinkErrorSink errorSink, PlanContext context) {
    createErrorStream(context).ifPresent(errorStream -> registerErrors(
        errorStream, errorSink, context));

    return null;
  }

  @Override
  public Object visitJarStatement(FlinkJarStatement statement, PlanContext context) {
    TableResult result = context.getTEnv().executeSql(String.format("ADD JAR '%s'", statement.getPath()));
    return null;
  }

  @Builder
  public static class SourceResolver {

    DataSystemConnectorConfig config;

    String format;

    Object watermark;

    Object schema;


    @Value
    public static class SourceResultResult {

      DataStream dataStream;
      List<SideOutputDataStream> errorStream;
    }
  }


  @Override
  public Object visitTableDefinition(FlinkDataStreamDefinition table, PlanContext context) {
    DataStream dataStream = buildStream(table.getConfig(),
        null,
        new FlinkTextLineFormat(),
        new FlexibleTableSchemaFactory(),
        table.getSchemaDefinition(),
        table.getOutputSchema(),
        context);

    log.debug("Creating datastream table definition: {}", table.getName());
    context.getTEnv()
        .createTemporaryView(table.getName(), dataStream, (Schema) null/*todo add schema*/);

    return null;
  }

  public DataStream buildStream(
      TableConfig tableConfig,
      SourceFactory<SingleOutputStreamOperator<TimeAnnotatedRecord<String>>> sourceFactory,
      FlinkFormatFactory formatFactory,
      TableSchemaFactory factory,
      SchemaDefinition definition,
      TypeInformation outputSchema,
      PlanContext context) {
    final List<SideOutputDataStream> errorSideChannels = new ArrayList<>();

    DataSystemConnector connector = tableConfig.getConnector().initialize(ErrorCollector.root());

    SingleOutputStreamOperator<TimeAnnotatedRecord<String>> stream =
        sourceFactory.create(connector,
            new FlinkSourceFactoryContext(context.getSEnv(), "nameStub",
                tableConfig.getFormat(), tableConfig, UUID.randomUUID()));

    Format.Parser parser = tableConfig.getFormat().getImplementation()
        .getParser(tableConfig.getFormat());

    OutputTag formatErrorTag = context.createErrorTag();
    FunctionWithError<TimeAnnotatedRecord<String>, Raw> formatStream = formatFactory
        .create(parser, stream, formatErrorTag);
    SingleOutputStreamOperator process = stream.process(
        new MapWithErrorProcess<>(formatErrorTag, formatStream,
            ErrorPrefix.INPUT_DATA.resolve("format")),
        TypeInformation.of(Raw.class));
    errorSideChannels.add(process.getSideOutput(formatErrorTag));

    //todo validator may be optional
    TableSchema schema = factory.create(definition)
        .get();
    //todo: fix flexible schema hard referenced
    DefaultSchemaValidator schemaValidator = (DefaultSchemaValidator)schema.getValidator(tableConfig.getSchemaAdjustmentSettings(),
        false);

    //validate schema
    OutputTag errorTag = context.createErrorTag();
    SingleOutputStreamOperator<Named> schemaValidatedStream = process.process(
        new MapWithErrorProcess<>(errorTag,
            new Function(schemaValidator),
            ErrorPrefix.INPUT_DATA.resolve("schemaValidation")),
        TypeInformation.of(Named.class));
    errorSideChannels.add(
        schemaValidatedStream.getSideOutput(errorTag));

    //Map rows (from factory)
    FlexibleSchemaRowMapper mapper =
        new FlexibleSchemaRowMapper(schema, false,
            FlinkRowConstructor.INSTANCE);

    DataStream rows = schemaValidatedStream
        .map(mapper::apply, outputSchema);

    context.getErrorStreams().addAll(errorSideChannels);

    return rows;
  }

  @AllArgsConstructor
  public static class Function implements FunctionWithError<SourceRecord.Raw, SourceRecord.Named> {

    private final DefaultSchemaValidator validator;

    @Override
    public Optional<SourceRecord.Named> apply(SourceRecord.Raw raw,
        Supplier<ErrorCollector> errorCollectorSupplier) {
      ErrorCollector errors = errorCollectorSupplier.get();
      SourceRecord.Named result = validator.verifyAndAdjust(raw, errors);
      if (errors.isFatal()) {
        return Optional.empty();
      } else {
        return Optional.of(result);
      }
    }
  }
  @Override
  public Object visitTableDefinition(FlinkSqlTableApiDefinition table, PlanContext context) {
    log.debug("Creating table definition: {}", table.getCreateSql());

    return context.getTEnv().executeSql(table.getCreateSql());
  }

  @Override
  public Object visitFactoryDefinition(FlinkFactoryDefinition table, PlanContext context) {
    log.debug("Creating factory: {} {}", table.getName(), table.getFactoryClass());

    String name = table.getName();
    Class factoryClass = table.getFactoryClass();
    DataSystemConnectorConfig config = table.getConfig();
    TableConfig tableConfig = table.getTableConfig();

    BaseConnectorFactory factory = createFactoryInstance(factoryClass);
    if (factory instanceof SourceFactory) {
      SourceFactory sourceFactory = (SourceFactory) factory;

      DataStream dataStream = buildStream(table.getTableConfig(),
          sourceFactory,
          new FlinkTextLineFormat(),
          new FlexibleTableSchemaFactory(),
          table.getSchemaDefinition(),
          table.getTypeInformation(),
          context);

      context.getTEnv().createTemporaryView(name, dataStream, toSchema(table.getSchema()));

    } else if (factory instanceof SinkFactory) {
      SinkFactory sinkFactory = (SinkFactory) factory;
      Object o = sinkFactory.create(
          new FlinkSinkFactoryContext(name, config, tableConfig, context.getErrors()));
      if (o instanceof DataStream) {
        throw new RuntimeException("Datastream as sink not yet implemented");
      } else if (o instanceof TableDescriptor.Builder) {
        TableDescriptor.Builder builder = (TableDescriptor.Builder) o;
        TableDescriptor descriptor = builder.schema(toSchema(table.getSchema()))
            .build();
        context.getTEnv().createTemporaryTable(name, descriptor);
      } else {
        throw new RuntimeException("Unknown sink type");
      }

    } else {
      throw new RuntimeException("Unknown factory type " + factory.getClass().getName());
    }

    return null;
  }


  public static BaseConnectorFactory createFactoryInstance(Class<?> factoryClass) {
    try {
      //todo generics
      Constructor constructor = factoryClass.getDeclaredConstructor();
      return (BaseConnectorFactory) constructor.newInstance();
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException |
             InvocationTargetException e) {
      throw new RuntimeException("Could not load connector: " + factoryClass.getName());
    }
  }

  @Value
  public static class PlanContext {

    ErrorCollector errors = ErrorCollector.root();
    StreamExecutionEnvironment sEnv;
    StreamTableEnvironment tEnv;

    StatementSet statementSet;

    List<SideOutputDataStream> errorStreams = new ArrayList<>();
    AtomicInteger errorTagCount = new AtomicInteger(0);

    public OutputTag<InputError> createErrorTag() {
      return new OutputTag<>(ERROR_TAG_PREFIX + "#" + errorTagCount.incrementAndGet()) {
      };
    }
  }
}
