package com.datasqrl;

import com.datasqrl.FlinkEnvironmentBuilder.InputValidatorFunction;
import com.datasqrl.FlinkExecutablePlan.DefaultFlinkConfig;
import com.datasqrl.FlinkExecutablePlan.FlinkBase;
import com.datasqrl.FlinkExecutablePlan.FlinkBaseVisitor;
import com.datasqrl.FlinkExecutablePlan.FlinkConfigVisitor;
import com.datasqrl.FlinkExecutablePlan.FlinkErrorSink;
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
import com.datasqrl.InputError.InputErrorMessage;
import com.datasqrl.InputError.Map2InputErrorMessage;
import com.datasqrl.StreamTableConverter.ConvertToStream;
import com.datasqrl.StreamTableConverter.EmitFirstInsertOrUpdate;
import com.datasqrl.StreamTableConverter.KeyedIndexSelector;
import com.datasqrl.StreamTableConverter.RowMapper;
import com.datasqrl.config.BaseConnectorFactory;
import com.datasqrl.config.DataStreamSourceFactory;
import com.datasqrl.config.FlinkSinkFactoryContext;
import com.datasqrl.config.FlinkSourceFactoryContext;
import com.datasqrl.config.SinkFactory;
import com.datasqrl.config.TableDescriptorSinkFactory;
import com.datasqrl.config.TableDescriptorSourceFactory;
import com.datasqrl.engine.stream.FunctionWithError;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.error.NotYetImplementedException;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.SourceRecord.Named;
import com.datasqrl.io.SourceRecord.Raw;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.tables.SchemaValidator;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.datasqrl.model.LogicalStreamMetaData;
import com.datasqrl.model.StreamType;
import com.datasqrl.serializer.SerializableSchema;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.CodeBlock.Builder;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeSpec;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;
import lombok.SneakyThrows;
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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

@Slf4j
public class FlinkCodeGen implements
    FlinkExecutablePlanVisitor<TypeSpec, Object>,
    FlinkBaseVisitor<TypeSpec, Object>,
    FlinkConfigVisitor<CodeBlock, Object>,
    FlinkStatementVisitor<CodeBlock, PlanContext>,
    FlinkFunctionVisitor<CodeBlock, PlanContext>,
    FlinkTableDefinitionVisitor<CodeBlock, PlanContext>,
    FlinkQueryVisitor<CodeBlock, PlanContext>,
    FlinkSinkVisitor<CodeBlock, PlanContext>
//    FlinkErrorSinkVisitor<CodeBlock, PlanContext>,
{

  public static final String ERROR_SINK_NAME = "errors_internal_sink";

  private final ErrorCollector errors;

  List<MethodSpec> fncs = new ArrayList<>();

  public FlinkCodeGen(ErrorCollector errors) {
    this.errors = errors;
  }

  public FlinkCodeGen() {
    this(ErrorCollector.root());
  }

  @Override
  public TypeSpec visitPlan(FlinkExecutablePlan plan, Object context) {
    return plan.getBase().accept(this, null);
  }

  @Override
  public TypeSpec visitBase(FlinkBase base, Object context) {
    TypeSpec.Builder flink = TypeSpec
        .classBuilder("FlinkMain")
        .addModifiers(Modifier.PUBLIC);

    flink.addField(FieldSpec.builder(ObjectMapper.class,
            "objectMapper", Modifier.PUBLIC, Modifier.STATIC)
        .initializer(CodeBlock.builder()
            .addStatement("$T.INSTANCE", SqrlObjectMapper.class)
            .build())
        .build());

    flink.addMethod(MethodSpec.methodBuilder("main")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(void.class)
        .addParameter(String[].class, "args")
        .addStatement("new FlinkMain().run()")
        .addException(Exception.class)
        .build());

    PlanContext ctx = new PlanContext();

    MethodSpec.Builder run = MethodSpec
        .methodBuilder("run")
        .addException(Exception.class)
        .addModifiers(Modifier.PUBLIC);

    CodeBlock env = base.getConfig().accept(this, context);

    run.addCode(env)
        .addCode("$L", "\n");

    base.getStatements()
        .forEach(f -> run.addCode(f.accept(this, ctx)).addCode("$L", "\n"));
    base.getFunctions()
        .forEach(f -> run.addCode(f.accept(this, ctx)).addCode("$L", "\n"));
    base.getTableDefinitions()
        .forEach(f -> run.addCode(f.accept(this, ctx)).addCode("$L", "\n"));
    base.getQueries()
        .forEach(f -> run.addCode(f.accept(this, ctx)).addCode("$L", "\n"));
    base.getSinks()
        .forEach(f -> run.addCode(f.accept(this, ctx)).addCode("$L", "\n"));

    run.addStatement("statementSet.execute()");

    MethodSpec runMethod = run.build();
    flink.addMethod(runMethod);

    for (MethodSpec methodSpec : fncs) {
      flink.addMethod(methodSpec);
    }

    return flink.build();
  }

  @Override
  public CodeBlock visitConfig(DefaultFlinkConfig config, Object context) {
    CodeBlock.Builder code = CodeBlock.builder();
    code
        .addStatement("$T<String, String> streamConfig = new $T<>()", Map.class, HashMap.class);
    for (Map.Entry<String, String> sConfig : config.getStreamExecutionEnvironmentConfig()
        .entrySet()) {
      code.addStatement("streamConfig.put($S, $S)", sConfig.getKey(), sConfig.getValue());
    }
    code.addStatement("$T sEnvConfig = $T.fromMap(streamConfig)", Configuration.class,
            Configuration.class)
        .addStatement("$T sEnv = $T.getExecutionEnvironment(sEnvConfig)",
            StreamExecutionEnvironment.class, StreamExecutionEnvironment.class)
//        .addStatement("$T objectMapper = new $T()",
//            ObjectMapper.class, ObjectMapper.class)
        .addStatement("$T<String, String> tableConfig = new $T<>()", Map.class, HashMap.class);
    for (Map.Entry<String, String> tConfig : config.getTableEnvironmentConfig().entrySet()) {
      code.addStatement("tableConfig.put($S, $S)", tConfig.getKey(), tConfig.getValue());
    }
    code.addStatement(
            "$T tEnvConfig = $T.newInstance().withConfiguration($T.fromMap(tableConfig)).build()",
            EnvironmentSettings.class, EnvironmentSettings.class, Configuration.class)
        .addStatement("$T tEnv = $T.create(sEnv, tEnvConfig)", StreamTableEnvironment.class,
            StreamTableEnvironment.class);

    code.addStatement("$T statementSet = tEnv.createStatementSet()", StatementSet.class);
    return code.build();
  }

  @Override
  public CodeBlock visitFunction(FlinkJavaFunction fnc, PlanContext context) {
    return CodeBlock.builder().addStatement(
            "tEnv.executeSql(\"CREATE TEMPORARY FUNCTION IF NOT EXISTS $L AS '$L' LANGUAGE JAVA\")",
            fnc.getFunctionName(),
            fnc.getIdentifier())
        .build();
  }

  @Override
  public CodeBlock visitFunction(FlinkSqlFunction fnc, PlanContext context) {
    return CodeBlock.builder().addStatement(
            "tEnv.executeSql($S)", fnc.getFunctionSql())
        .build();
  }

  @Override
  public CodeBlock visitSink(FlinkSqlSink table, PlanContext context) {
    return CodeBlock.builder().addStatement(
            "statementSet.addInsert($S, tEnv.from($S))", table.getTarget(), table.getSource())
        .build();
  }

  //
  @Override
  public CodeBlock visitQuery(FlinkSqlQuery query, PlanContext context) {
    return CodeBlock.builder()
        .addStatement("tEnv.createTemporaryView($S, tEnv.sqlQuery($S))", query.getName(),
            query.getQuery())
        .build();
  }

  @Override
  public CodeBlock visitQuery(FlinkStreamQuery query, PlanContext context) {

    int tableInt = i.incrementAndGet();
    int streamInt = i.incrementAndGet();
    CodeBlock.Builder code = CodeBlock.builder()
        .addStatement("$T table$L = tEnv.from($S)", Table.class, tableInt, query.getFromTable())
        .addStatement(
            "$T<$T> stream$L = tEnv.toChangelogStream(table$L, table$L.getSchema().toSchema(), $T.upsert())",
            DataStream.class, Row.class, streamInt, tableInt, tableInt, ChangelogMode.class);
    StreamType streamType = query.getStateChangeType();
    LogicalStreamMetaData baseRelationMeta = query.getMeta();
    boolean unmodifiedChangelog = query.isUnmodifiedChangelog();
    log.debug("Creating Stream query {}, {}", query.getName(), query.getFromTable());

    switch (streamType) {
      case ADD:
        if (unmodifiedChangelog) {
          //We can simply filter on RowKind
          code.addStatement("stream$L = stream$L.filter(row -> row.getKind() == $T.INSERT)",
              streamInt, streamInt, RowKind.class);
        } else {
          //Only emit the first insert or update per key
          code.addStatement(
              "stream$L = stream$L.keyBy(new $T(new int[]{$L})).flatMap(new $T())",
              streamInt, streamInt, KeyedIndexSelector.class,
              Arrays.stream(baseRelationMeta.getKeyIdx())
                  .mapToObj(Integer::toString)
                  .collect(Collectors.joining(",")), EmitFirstInsertOrUpdate.class);
        }
        break;
      case DELETE:
        Preconditions.checkArgument(unmodifiedChangelog,
            "Cannot create DELETE stream from modified state table. Invert filter and use ADD instead.");
        code.addStatement("stream$L = stream$L.filter(row -> row.getKind() == $T.DELETE)",
            streamInt, streamInt, RowKind.class);
        break;
      case UPDATE:
        code.addStatement(
            "stream$L = stream$L.filter(row -> row.getKind() == $T.INSERT || row.getKind() == $T.UPDATE_AFTER)",
            streamInt, streamInt, RowKind.class, RowKind.class);
        break;
      default:
        throw new UnsupportedOperationException("Unexpected state change type: " + streamType);
    }

    String schemaName = "schema" + i.incrementAndGet();
    toSchema(code, query.getSchema(), schemaName);

    String typeInfoName = "typeInfo" + i.incrementAndGet();
    toRowTypeInfo(code, query.getTypeInformation(), typeInfoName);

    CodeBlock codeBlock = code.addStatement(
            "$T rowMapper = new $T(new $T(new int[]{$L}, new int[]{$L}, $L))",
            RowMapper.class,
            RowMapper.class,
            LogicalStreamMetaData.class,
            Arrays.stream(baseRelationMeta.getKeyIdx())
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(",")),
            Arrays.stream(baseRelationMeta.getSelectIdx())
                .mapToObj(Integer::toString)
                .collect(Collectors.joining(",")),
            baseRelationMeta.getTimestampIdx())
        .addStatement("stream$L = stream$L.process(new $T(rowMapper), $L)",
            streamInt, streamInt,
            ConvertToStream.class,
            typeInfoName)
        .addStatement("tEnv.createTemporaryView($S, stream$L, $L)",
            query.getName(),
            streamInt,
            schemaName)
        .build();

    return codeBlock;
  }

  //See flink's docs: docs/content/docs/dev/datastream/fault-tolerance/serialization/types_serialization.md
  @SneakyThrows
  private void toRowTypeInfo(Builder code, TypeInformation typeInformation, String typeInfoName) {

    String typeInfo = SqrlObjectMapper.INSTANCE.writerWithDefaultPrettyPrinter()
        .writeValueAsString(typeInformation);

    code.addStatement("$T $L = objectMapper.readValue($S, $T.class)",
        TypeInformation.class,
        typeInfoName,
        typeInfo,
        TypeInformation.class
    );
  }

  AtomicInteger i = new AtomicInteger();

  //
  private CodeBlock toSchema(Builder code, SerializableSchema schema, String name) {
    String builderName = name + "Builder";
    code.addStatement("$T $L = $T.newBuilder()", Schema.Builder.class, builderName, Schema.class);

    for (Pair<String, DataType> column : schema.getColumns()) {
      code.addStatement("$L.column($S, $S)", builderName, column.getKey(), column.getValue());
    }

    if (!Strings.isNullOrEmpty(schema.getWatermarkExpression())) {
      code.addStatement("$L.columnByExpression($S, $S)", builderName, schema.getWatermarkName(),
          schema.getWatermarkExpression());
    }

    switch (schema.getWaterMarkType()) {
      case COLUMN_BY_NAME:
        String boundedUnorderedNess = " - INTERVAL '1' SECOND";
        code.addStatement("$L.watermark($S, $S)", builderName, schema.getWatermarkName(),
            "`" + schema.getWatermarkName() + "`" + boundedUnorderedNess);
        break;
      case SOURCE_WATERMARK:
        code.addStatement("$L.watermark($S, SOURCE_WATERMARK())", builderName,
            schema.getWatermarkName());
        break;
    }

    if (schema.getPrimaryKey() != null && !schema.getPrimaryKey().isEmpty()) {
      code.addStatement("$L.primaryKey($L)", builderName, schema.getPrimaryKey().stream()
          .map(k->"\""+k+"\"").collect(Collectors.joining(",")));
    }

    code.addStatement("$T $L = $L.build()", Schema.class, name, builderName);

    return code.build();
  }

  @Override
  public CodeBlock visitJarStatement(FlinkJarStatement statement, PlanContext context) {
    return CodeBlock.builder()
        .addStatement("tEnv.executeSql(\"ADD JAR '$S'\")", statement.getPath())
        .build();
  }

  @Override
  public CodeBlock visitTableDefinition(FlinkSqlTableApiDefinition table, PlanContext context) {
    return CodeBlock.builder()
        .addStatement("tEnv.executeSql($S)", table.getCreateSql())
        .build();
  }


  @SneakyThrows
  @Override
  public CodeBlock visitFactoryDefinition(FlinkFactoryDefinition table, PlanContext context) {
    String name = table.getName();
    TableConfig tableConfig = table.getTableConfig().deserialize(errors);

    FormatFactory formatFactory = createFactoryInstance(table.getFormatFactory());
    BaseConnectorFactory connectorFactory = createFactoryInstance(table.getConnectorFactory());
    if (connectorFactory instanceof DataStreamSourceFactory) {
      System.out.println("Datastream...");

      DataStreamSourceFactory sourceFactory = (DataStreamSourceFactory) connectorFactory;
      TableSchemaFactory factory = createFactoryInstance(table.getSchemaFactory());
      String schemaDefinition = table.getSchemaDefinition();
      TypeInformation outputSchema = table.getTypeInformation();
      final List<SideOutputDataStream> errorSideChannels = new ArrayList<>();
      Builder builder = CodeBlock.builder();

      deserializeTableConfig("tableConfig", tableConfig, builder);

      builder
          .addStatement("$T<$T<String>> stream =\n"
                  + "new $T().create(new $T(sEnv, $S, tableConfig.serialize(),\n"
                  + "new $T(), $T.randomUUID()))",
              SingleOutputStreamOperator.class,
              TimeAnnotatedRecord.class,
              sourceFactory.getClass(),
              FlinkSourceFactoryContext.class,
              tableConfig.getName().getCanonical(),
              formatFactory.getClass(),
              UUID.class)
          .build();
//

      OutputTag formatErrorTag = context.createErrorTag();
      builder.addStatement("$T formatErrorTag = new $T<>($S) {\n"
              + "      }; ",
          OutputTag.class,
          OutputTag.class,
          formatErrorTag.getId());
      builder.addStatement("$T flinkFormat = "
              + "$T.of(new $T().getParser(tableConfig.getFormatConfig()))",
          FlinkFormatFactory.class,
          FlinkFormatFactory.class,
          formatFactory.getClass()
      );
      builder.addStatement(
          "$T<$T<String>, $T> formatStream = flinkFormat.create(stream, formatErrorTag)",
          FunctionWithError.class,
          TimeAnnotatedRecord.class,
          Raw.class);

      builder.addStatement("$T process = stream.process(\n"
              + "          new $T<>(formatErrorTag, formatStream,\n"
              + "              $T.INPUT_DATA.resolve(\"format\")),\n"
              + "          $T.of($T.class))",
          SingleOutputStreamOperator.class,
          MapWithErrorProcess.class,
          ErrorPrefix.class,
          TypeInformation.class,
          Raw.class);

      //todo validator may be optional
      TableSchema tableSchema = factory.create(schemaDefinition,
          tableConfig.getBase().getCanonicalizer());
      String schemaName = "schema" + i.incrementAndGet();
      builder.addStatement(
          "$T $L = ($T)new $T().create($S, tableConfig.getBase().getCanonicalizer())",
          tableSchema.getClass(),
          schemaName,
          tableSchema.getClass(),
          table.getSchemaFactory(),
          schemaDefinition);

//      TableSchema schema = factory.create(schemaDefinition, tableConfig.getBase().getCanonicalizer());
      //todo: fix flexible schema hard referenced
      builder.addStatement("$T schemaValidator = $L.getValidator(\n"
              + "          tableConfig.getSchemaAdjustmentSettings(),\n"
              + "          tableConfig.getConnectorSettings())",
          SchemaValidator.class,
          schemaName);
      OutputTag errorTag = context.createErrorTag();
      builder.addStatement("$T errorTag = new $T<>($S) {\n"
              + "      }; ",
          OutputTag.class,
          OutputTag.class,
          errorTag.getId());

      builder.addStatement("$T<$T> schemaValidatedStream = process.process(\n"
              + "        new $T<>(errorTag,\n"
              + "            new $T(schemaValidator),\n"
              + "            $T.INPUT_DATA.resolve(\"schemaValidation\")),\n"
              + "        $T.of($T.class));\n"
              + "    ",
          SingleOutputStreamOperator.class,
          Named.class,
          MapWithErrorProcess.class,
          InputValidatorFunction.class,
          ErrorPrefix.class,
          TypeInformation.class,
          SourceRecord.Named.class);

      builder.addStatement("$T mapper = $L.getRowMapper(\n"
              + "          $T.INSTANCE, tableConfig.getConnectorSettings())",

          com.datasqrl.engine.stream.RowMapper.class,
          schemaName,
          FlinkRowConstructor.class);

      String rowTypeName = "type" + i.incrementAndGet();
      toRowTypeInfo(builder, outputSchema, rowTypeName);

      builder.addStatement("$T rows = schemaValidatedStream\n"
              + "          .map(mapper::apply, $L)",
          DataStream.class,
          rowTypeName);

//      context.getErrorStreams().addAll(errorSideChannels);
      toSchema(builder, table.getSchema(), "schema");

      builder.addStatement("tEnv.createTemporaryView($S, rows, schema)",
          table.getName());

      String methodName = "create" + (i.incrementAndGet());
      MethodSpec create = MethodSpec.methodBuilder(methodName)
          .addCode(builder.build())
          .addParameter(ParameterSpec.builder(StreamExecutionEnvironment.class,
                  "sEnv")
              .build())
          .addParameter(ParameterSpec.builder(StreamTableEnvironment.class,
                  "tEnv")
              .build())
          .addException(Exception.class)
          .build();
      fncs.add(create);

      return CodeBlock.builder()
          .addStatement("$L(sEnv, tEnv)", methodName)
          .build();
    } else if (connectorFactory instanceof TableDescriptorSourceFactory) {
      String sourceFactory = "sourceFactory" + i.incrementAndGet();
      CodeBlock.Builder builder = CodeBlock.builder()
          .addStatement("$T $L = ($T) connectorFactory",
              TableDescriptorSourceFactory.class,
              sourceFactory,
              TableDescriptorSourceFactory.class);
      String builderName = "builder" + i.incrementAndGet();

      String tableConfigName = "tableConfig" + i.incrementAndGet();
      deserializeTableConfig(tableConfigName, tableConfig, builder);

      builder.addStatement(
          "$T.Builder $L = sourceFactory.create(new $T(sEnv, name, $L.serialize(), formatFactory, $T.randomUUID()))",
          TableDescriptor.class,
          builderName,
          FlinkSourceFactoryContext.class,
          tableConfigName,
          UUID.class);
      String schemaName = "schema" + i.incrementAndGet();
      toSchema(builder, table.getSchema(), schemaName);
      builder.addStatement("$T descriptor = $L.schema(toSchema(table.getSchema())).build()",
              TableDescriptor.class,
              builderName)
          .addStatement("tEnv.createTemporaryTable(name, descriptor)");
      return builder
          .build();
    } else if (connectorFactory instanceof SinkFactory) {

      SinkFactory sinkFactory = (SinkFactory) connectorFactory;
      Object o = sinkFactory.create(
          new FlinkSinkFactoryContext(name, tableConfig, formatFactory));
      if (o instanceof DataStream) {
        throw new NotYetImplementedException("Datastream as sink not yet implemented");
      } else if (o instanceof TableDescriptor.Builder) {
        Builder builder = CodeBlock.builder();
        String sinkFactoryName = "sinkFactory" + i.incrementAndGet();
        builder.addStatement("$T $L = new $T()",
            table.getConnectorFactory(),
            sinkFactoryName,
            table.getConnectorFactory());

        String tableConfigName = "tableConfig" + i.incrementAndGet();
        deserializeTableConfig(tableConfigName, tableConfig, builder);

        String builderName = "builder" + i.incrementAndGet();
        builder.addStatement("$T.Builder $L = $L.create("
                + "   new $T($S, $L, new $T()))",
            TableDescriptor.class,
            builderName,
            sinkFactoryName,
            FlinkSinkFactoryContext.class,
            name,
            tableConfigName,
            table.getFormatFactory());

        String schemaName = "schema" + i.incrementAndGet();
        toSchema(builder, table.getSchema(), schemaName);
        String descriptorName = "descriptor" + i.incrementAndGet();
        builder
            .addStatement("$T $L = $L.schema($L).build()",
                TableDescriptor.class,
                descriptorName,
                builderName,
                schemaName)
            .addStatement("tEnv.createTemporaryTable($S, $L)",
                name,
                descriptorName)
        ;
        return builder.build();
      } else {
        throw new RuntimeException("Unknown sink type");
      }

    } else {
      throw new RuntimeException("Unknown factory type " + connectorFactory.getClass().getName());
    }
  }

  @SneakyThrows
  private void deserializeTableConfig(String varName, TableConfig config, Builder builder) {
    String sqrlConfig = varName + "SqrlConfig";
    builder.addStatement("$T $L = objectMapper.readValue($S, $T.class)",
        TableConfig.Serialized.class,
        sqrlConfig,
        SqrlObjectMapper.INSTANCE.writerWithDefaultPrettyPrinter()
            .writeValueAsString(config.serialize()),
        TableConfig.Serialized.class);
    builder.addStatement("$T $L = $L.deserialize($T.root())",
        TableConfig.class,
        varName,
        sqrlConfig,
        ErrorCollector.class);
  }

  //  @Override
//  public Object visitErrorSink(FlinkErrorSink errorSink, FlinkEnvironmentBuilder.PlanContext context) {
//    createErrorStream(context).ifPresent(errorStream -> registerErrors(
//        errorStream, errorSink, context));
//
//    return null;
//  }
//
  private Optional<DataStream<InputError>> createErrorStream(
      FlinkEnvironmentBuilder.PlanContext planCtx) {
    List<DataStream> errorStreams = planCtx.getErrorStreams();
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

  private void registerErrors(DataStream<InputError> errorStream, FlinkErrorSink sink,
      FlinkEnvironmentBuilder.PlanContext context) {

    DataStream<InputErrorMessage> errorMessages = errorStream.flatMap(new Map2InputErrorMessage());
    Schema errorTableSchema = InputError.InputErrorMessage.getTableSchema();
    Table errorTable = context.getTEnv().fromDataStream(errorMessages, errorTableSchema);

    TableDescriptorSinkFactory builderClass = (TableDescriptorSinkFactory) createFactoryInstance(
        sink.getConnectorFactory());
    FormatFactory formatFactory = createFactoryInstance(sink.getFormatFactory());
    TableDescriptor descriptor = builderClass.create(new FlinkSinkFactoryContext(sink.getName(),
            sink.getTableConfig().deserialize(errors), formatFactory))
        .schema(errorTableSchema)
        .build();

    context.getTEnv().createTemporaryTable(ERROR_SINK_NAME, descriptor);

    context.getStatementSet().addInsert(ERROR_SINK_NAME, errorTable);
  }

  public static <T> T createFactoryInstance(Class<T> factoryClass) {
    try {
      Constructor<T> constructor = factoryClass.getDeclaredConstructor();
      return constructor.newInstance();
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException |
             InvocationTargetException e) {
      throw new RuntimeException("Could not load factory for: " + factoryClass.getName());
    }
  }
}
