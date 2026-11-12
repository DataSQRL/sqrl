/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.engine.log.kafka;

import static com.google.common.base.Preconditions.checkArgument;

import com.datasqrl.config.ConnectorConf;
import com.datasqrl.config.ConnectorConf.Context;
import com.datasqrl.config.ConnectorConfImpl;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.config.TestRunnerConfiguration;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.flink.json.FlexibleJsonFlinkFormatTypeMapper;
import com.datasqrl.deployment.model.KafkaIncludedTopicModel;
import com.datasqrl.deployment.model.KafkaNewTopicModel;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.flinkrunner.connector.kafka.KeyFieldsOptions;
import com.datasqrl.flinkrunner.format.json.FlexibleJsonFormat;
import com.datasqrl.io.schema.avro.AvroToRelDataTypeConverter;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan.Query;
import com.datasqrl.planner.parser.NoLocationStatementParserException;
import com.datasqrl.planner.tables.FlinkConnectorConfigWrapper;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import com.datasqrl.server.MutationInsertType;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.StreamUtil;
import com.google.common.collect.Streams;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlMetadataColumn;
import org.apache.flink.util.TimeUtils;

@Slf4j
public class KafkaLogEngine extends ExecutionEngine.Base implements LogEngine {

  public static final String CONNECTOR_TOPIC_KEY = "topic";
  public static final String UPSERT_FORMAT = "upsert-%s";
  public static final String SUBSCRIPTION_TOPIC_SUFFIX = "-subscription";

  public static final String DEFAULT_TTL_KEY = "retention";

  public static final EnumSet<EngineFeature> KAFKA_FEATURES =
      EnumSet.of(EngineFeature.MUTATIONS, EngineFeature.SHARED_WITH_STANDALONE_INCLUDES);

  private static final String TABLE_ID_VARIABLE = "table-id";

  @Getter private final EngineConfig engineConfig;
  private final ErrorCollector errors;
  private final KafkaFormatConfigurer formats;
  private final TestRunnerConfiguration testRunnerConfig;
  private final ConnectorConf streamConnectorConf;
  private final ConnectorConf mutationConnectorConf;
  private final List<String> standaloneIncludes;
  // The topics of the mutation tables, with the standalone include that owns each, or empty if this
  // project owns it. Filled by createMutation, which runs for every mutation before the planner
  // calls createTable
  private final Map<String, Optional<String>> mutationTopicOwners = new HashMap<>();

  // === SETTINGS ===
  private final Optional<Duration> defaultTTL;
  private final boolean useSourceWatermark;
  private final boolean useTransactionSourceWatermark;
  private final Duration defaultWatermark;
  private final Duration transactionWatermark;
  private final int numPartitions;
  private final short replicationFactor;
  private final String format;

  @Inject
  public KafkaLogEngine(
      PackageJson json, ConnectorFactoryFactory connectorFactory, ErrorCollector errors) {
    super(KafkaLogEngineFactory.ENGINE_NAME, EngineType.LOG, KAFKA_FEATURES);
    this.engineConfig = json.getEngines().getEngineConfigOrEmpty(KafkaLogEngineFactory.ENGINE_NAME);
    this.errors = errors;
    this.formats = new KafkaFormatConfigurer(errors);
    this.testRunnerConfig = json.getTestConfig();
    this.streamConnectorConf = connectorFactory.getConfig(KafkaLogEngineFactory.ENGINE_NAME);
    this.mutationConnectorConf =
        connectorFactory.getConfig(KafkaLogEngineFactory.ENGINE_NAME + "-mutation");
    this.standaloneIncludes = json.getScriptConfig().getStandaloneIncludes();
    defaultTTL =
        engineConfig
            .getPropertyOptional(DEFAULT_TTL_KEY)
            .map(
                value ->
                    value.equals("-1") ? Duration.ofMillis(-1) : TimeUtils.parseDuration(value));
    useSourceWatermark = Boolean.parseBoolean(engineConfig.getProperty("use-source-watermark"));
    useTransactionSourceWatermark =
        Boolean.parseBoolean(engineConfig.getProperty("use-transaction-source-watermark"));
    defaultWatermark = TimeUtils.parseDuration(engineConfig.getProperty("watermark"));
    transactionWatermark =
        TimeUtils.parseDuration(engineConfig.getProperty("transaction-watermark"));
    numPartitions = Integer.parseInt(engineConfig.getProperty("num-partitions"));
    replicationFactor = Short.parseShort(engineConfig.getProperty("replication-factor"));
    format =
        String.valueOf(KafkaFormatConfigurer.valueFormat(streamConnectorConf.toMap()))
            .trim()
            .toLowerCase();
  }

  @Override
  public MutationCreateTable createMutation(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      MutationSettings settings) {
    return createInternal(
        stage,
        originalTableName,
        tableBuilder,
        relDataType,
        Optional.empty(),
        Optional.of(settings),
        false,
        Optional.empty());
  }

  @Override
  public EngineCreateTable createTable(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      TableAnalysis tableAnalysis) {
    return createInternal(
        stage,
        originalTableName,
        tableBuilder,
        relDataType,
        Optional.of(tableAnalysis),
        Optional.empty(),
        false,
        Optional.empty());
  }

  @Override
  public EngineCreateTable createTableForExportStatement(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      TableAnalysis tableAnalysis) {
    return createInternal(
        stage,
        originalTableName,
        tableBuilder,
        relDataType,
        Optional.of(tableAnalysis),
        Optional.empty(),
        true,
        Optional.empty());
  }

  /**
   * The table resolves its topic like {@link #createTable} does in the included project, which
   * plans the table with the same statement, so it names the topic that the included project
   * writes.
   */
  @Override
  public EngineCreateTable createIncludedTable(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      TableAnalysis tableAnalysis,
      String standaloneInclude) {
    // The table id is unique within a project, so the projects resolve different topics with it
    errors.checkFatal(
        !usesVariable(
            streamConnectorConf.toMap().getOrDefault(CONNECTOR_TOPIC_KEY, ""), TABLE_ID_VARIABLE),
        "Kafka table '%s' is written by the standalone include '%s', but the 'topic' option of"
            + " the 'kafka' connector uses '${sqrl:%s}', so this project cannot resolve the topic"
            + " that the include writes. Use '${sqrl:table-name}' instead.",
        originalTableName,
        standaloneInclude,
        TABLE_ID_VARIABLE);
    return createInternal(
        stage,
        originalTableName,
        tableBuilder,
        relDataType,
        Optional.of(tableAnalysis),
        Optional.empty(),
        false,
        Optional.of(standaloneInclude));
  }

  /**
   * @param mutation the settings of a mutation table, or empty for a table that Flink writes
   * @param includedBy the standalone include that writes a table that is not a mutation table, or
   *     empty if this project writes it
   */
  private Table createInternal(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      Optional<TableAnalysis> tableAnalysis,
      Optional<MutationSettings> mutation,
      boolean isExportStatement,
      Optional<String> includedBy) {

    boolean isMutation = mutation.isPresent();
    var standaloneInclude =
        mutation.flatMap(MutationSettings::standaloneInclude).or(() -> includedBy);
    boolean isTransactional =
        mutation
            .map(settings -> settings.insertType() == MutationInsertType.TRANSACTION)
            .orElse(false);
    var ttl = mutation.map(MutationSettings::ttl).orElse(defaultTTL);
    var ctxBuilder =
        Context.builder().tableName(originalTableName).tableId(tableBuilder.getTableName());
    var conf = isMutation ? mutationConnectorConf : streamConnectorConf;
    boolean isUpsert = false;
    var messageKey = List.<String>of();
    var topicConfig = new HashMap<String, String>();

    if (tableBuilder.hasPartition()) {
      // PARTITIONED BY only declares the Kafka message key. Flink's Kafka sink rejects partitioned
      // tables, so the clause must not reach Flink.
      messageKey = tableBuilder.getPartition();
      tableBuilder.removePartition();
    }

    if (tableBuilder.hasPrimaryKey()) {
      // Kafka only supports upserts on state tables
      if (tableAnalysis.map(TableAnalysis::getType).orElse(TableType.STATE).isState()) {
        isUpsert = true;
        // The primary key must be the partition key
        messageKey = tableBuilder.getPrimaryKey().get();
      } else {
        tableBuilder.removePrimaryKey();
      }
    }

    var connectorConfig = conf.toMapWithSubstitution(ctxBuilder.build());

    if (isMutation) {
      // Set watermark column for mutations based on 'timestamp' metadata
      for (SqlNode node : tableBuilder.getColumnList().getList()) {
        if (node instanceof SqlMetadataColumn metadataColumn) {
          setWatermark(tableBuilder, metadataColumn, connectorConfig, isTransactional);
        }
      }
    }

    // The message key is set in the connector options so that Flink writes it like the server does.
    var hasMessageKey = !messageKey.isEmpty();

    if (hasMessageKey && !isUpsert) {
      connectorConfig.put("key.fields", String.join(";", messageKey));
      // Decoding the key drops records without a key; 'kafka-safe' can read the key fields from
      // the value instead, since it holds every column ('value.fields-include' = 'ALL')
      var connector = connectorConfig.get(FlinkConnectorConfigWrapper.CONNECTOR_KEY);
      // TODO: Remove this '-safe' restriction and the warning once we update to an upstream Kafka
      // connector version that reads records without a key
      if (connector != null && connector.endsWith("-safe")) {
        connectorConfig.put(KeyFieldsOptions.SCAN_KEY_FIELDS_READ_FROM_VALUE.key(), "true");
      } else {
        errors.warn(
            "Kafka table '%s' uses the '%s' connector, which drops records without a message key"
                + " when reading them. Use 'kafka-safe' to keep them.",
            originalTableName, connector);
      }
    }

    if (isUpsert) {
      connectorConfig.put(
          FlinkConnectorConfigWrapper.CONNECTOR_KEY,
          UPSERT_FORMAT.formatted(connectorConfig.get(FlinkConnectorConfigWrapper.CONNECTOR_KEY)));
      // scan.startup.mode config option is invalid for upsert-kafka
      connectorConfig.remove("scan.startup.mode");
      topicConfig.put("cleanup.policy", "compact");
    }

    if (hasMessageKey) {
      connectorConfig.put(
          "value.fields-include", "ALL"); // it's slightly less efficient but easier for debugging
    }

    if (isTransactional) {
      checkArgument(isMutation, "Only mutations can be used for transactions: %s", tableBuilder);
      connectorConfig.put("properties.isolation.level", "read_committed");
    }
    ttl.ifPresent(duration -> topicConfig.put("retention.ms", String.valueOf(duration.toMillis())));
    // preserve any existing connector options
    connectorConfig.putAll(tableBuilder.getConnectorOptions());
    // after merging, so formats and format options given in WITH are configured as well
    var valueFormat =
        formats.configure(connectorConfig, originalTableName, hasMessageKey, isMutation);
    var topicName = connectorConfig.get(CONNECTOR_TOPIC_KEY);
    if (isMutation) {
      mutationTopicOwners.put(topicName, standaloneInclude);
    } else if (mutationTopicOwners.containsKey(topicName)) {
      // Only the mutation table writes its topic: another writer mixes its rows into the
      // mutation, and a writer that reads the mutation reads its own output in an endless loop
      errors.checkFatal(
          !isExportStatement,
          "Kafka table '%s' of an EXPORT statement writes into the topic '%s' of a mutation"
              + " table. Use a different table name in the EXPORT statement.",
          originalTableName,
          topicName);
      // A standalone include renames the topic only for its own mutation tables, the ones its
      // deployment knows, so it would write into the mutation topic of another project
      errors.checkFatal(
          includedBy.isEmpty() || mutationTopicOwners.get(topicName).equals(includedBy),
          "Kafka table '%s' is written by the standalone include '%s' into the topic '%s', which"
              + " is the topic of a mutation table that '%2$s' does not define. Rename one of"
              + " the tables.",
          originalTableName,
          includedBy.orElse(""),
          topicName);
      topicName =
          conf.toMapWithSubstitution(
                  ctxBuilder.tableName(originalTableName + SUBSCRIPTION_TOPIC_SUFFIX).build())
              .get(CONNECTOR_TOPIC_KEY);
      errors.checkFatal(
          !mutationTopicOwners.containsKey(topicName),
          "Kafka table '%s' writes into the topic '%s' of a mutation table. Use"
              + " '${sqrl:table-name}' in the 'topic' option of the 'kafka' connector.",
          originalTableName,
          topicName);
      connectorConfig.put(CONNECTOR_TOPIC_KEY, topicName);
    }
    tableBuilder.setConnectorOptions(connectorConfig);
    return new Table(
        topicName,
        tableBuilder.getTableName(),
        originalTableName,
        valueFormat,
        messageKey,
        relDataType,
        topicConfig,
        standaloneInclude);
  }

  @Override
  public DataTypeMapping getTypeMapping() {
    if (FlexibleJsonFormat.FORMAT_NAME.equalsIgnoreCase(format)) {
      return new FlexibleJsonFlinkFormatTypeMapper();
      //    keeping this for future avro support
      //    } else if (format.equalsIgnoreCase(AvroFlinkFormatTypeMapper.FORMAT_NAME)) {
      //      return new AvroFlinkFormatTypeMapper();
    } else {
      log.error("Unexpected format for Kafka log engine: {}", format);
      return DataTypeMapping.NONE;
    }
  }

  @Override
  public EnginePhysicalPlan plan(MaterializationStagePlan stagePlan) {
    Map<String, String> table2TopicMap =
        StreamUtil.filterByClass(stagePlan.getTables(), Table.class)
            .collect(Collectors.toMap(Table::tableName, Table::topicName));
    // Plan queries
    for (Query query : stagePlan.getQueries()) {
      var errors = query.errors();
      var relNode = query.relNode();
      Map<String, FunctionParameter> filterColumns = new HashMap<>();
      if (relNode instanceof Project project
          && relNode.getRowType().equals(project.getInput().getRowType())) {
        relNode = project.getInput();
      }
      if (relNode instanceof Filter filter) {
        Consumer<Boolean> checkErrors =
            b ->
                errors.checkFatal(
                    b,
                    "Expected simple equality condition for Kafka filter: %s",
                    filter.getCondition());
        relNode = filter.getInput();
        /*We expect that the filter is a simple AND condition of equality conditions of the sort `column = ?`
         i.e. a RexDynamicParam on one side and a RexInputRef on the other
        */
        var conditions = stagePlan.getUtils().rexUtil().getConjunctions(filter.getCondition());
        var fieldNames = filter.getRowType().getFieldNames();
        var sqrlFn = query.function();
        for (RexNode condition : conditions) {
          checkErrors.accept(condition instanceof RexCall);
          var call = (RexCall) condition;
          checkErrors.accept(call.getOperator().getKind() == SqlKind.EQUALS);
          for (var i = 0; i < 2; i++) {
            if (call.getOperands().get(i) instanceof RexDynamicParam) {
              var argumentIndex = ((RexDynamicParam) call.getOperands().get(i)).getIndex();
              var colIdx = CalciteUtil.getNonAlteredInputRef(call.getOperands().get((i + 1) % 2));
              checkErrors.accept(colIdx.isPresent());
              filterColumns.put(
                  fieldNames.get(colIdx.get()), sqrlFn.getParameters().get(argumentIndex));
            }
          }
        }
        checkErrors.accept(filterColumns.size() == conditions.size());
      }
      errors.checkFatal(
          relNode instanceof TableScan,
          "The Kafka engine currently only supports"
              + "simple filter queries without any transformations, but got: %s explain: %s",
          relNode.getClass().getName(),
          relNode.explain());
      errors.checkFatal(
          !query.function().isPassthrough(),
          "Kafka does not support passthrough queries: %s",
          query.function());
      RelOptTable table = relNode.getTable();
      var tableName = table.getQualifiedName().get(2);
      var topicName = table2TopicMap.get(tableName);
      checkArgument(
          topicName != null, "Could not find topic for table: %s [%s]", tableName, table2TopicMap);
      query
          .function()
          .setExecutableQuery(new KafkaQuery(stagePlan.getStage(), topicName, filterColumns));
    }
    // Plan topic creation. The topics that a standalone included project writes, of the mutation
    // tables that it defines and of the computed tables that it subscribes to, belong to that
    // project, so they are listed separately, without settings, from the topics this deployment
    // owns.
    var tablesByInclusion = partitionByInclusion(stagePlan.getTables());
    var mutationsByInclusion = partitionByInclusion(stagePlan.getMutations());

    var topics =
        Streams.concat(
                tablesByInclusion.get(false).stream()
                    .map(t -> createNewTopic(t, KafkaNewTopicModel.Type.SUBSCRIPTION)),
                mutationsByInclusion.get(false).stream()
                    .map(t -> createNewTopic(t, KafkaNewTopicModel.Type.MUTATION)))
            .toList();

    var testRunnerTopics =
        testRunnerConfig.getCreateTopics().stream()
            .map(topicName -> new KafkaNewTopic(new KafkaNewTopicModel(topicName, topicName)))
            .toList();

    var includedTopics =
        Streams.concat(
                tablesByInclusion.get(true).stream(), mutationsByInclusion.get(true).stream())
            .map(KafkaLogEngine::toIncludedTopic)
            .toList();
    noticeStandaloneIncludesWithoutTopics(includedTopics);

    return new KafkaPhysicalPlan(topics, testRunnerTopics, includedTopics);
  }

  private static boolean usesVariable(String template, String variable) {
    var matcher = ConnectorConfImpl.SQRL_VARIABLE_FINDER.matcher(template);
    while (matcher.find()) {
      if (matcher.group("identifier").equalsIgnoreCase(variable)) {
        return true;
      }
    }
    return false;
  }

  private static Map<Boolean, List<Table>> partitionByInclusion(
      List<? extends EngineCreateTable> tables) {
    return tables.stream()
        .map(Table.class::cast)
        .collect(Collectors.partitioningBy(table -> table.standaloneInclude().isPresent()));
  }

  private void noticeStandaloneIncludesWithoutTopics(List<KafkaIncludedTopicModel> includedTopics) {
    var includesWithTopics =
        includedTopics.stream().map(KafkaIncludedTopicModel::include).collect(Collectors.toSet());

    standaloneIncludes.stream()
        .filter(include -> !includesWithTopics.contains(include))
        .forEach(
            include ->
                errors.notice(
                    "Include '%1$s' sets \"standalone\": true, but this project uses no Kafka"
                        + " topic that '%1$s' writes, i.e. of an internal table that it defines or"
                        + " of a table that it subscribes to, so kafka.json lists no topic of"
                        + " '%1$s' under \"includedTopics\"",
                    include));
  }

  private void setWatermark(
      FlinkTableBuilder tableBuilder,
      SqlMetadataColumn metadataCol,
      Map<String, String> connectorConfig,
      boolean isTransactional) {

    var timestampMetadata =
        FlinkTableBuilder.metadataKey(metadataCol).equalsIgnoreCase("timestamp");

    if (!timestampMetadata || tableBuilder.hasWatermark()) {
      return;
    }

    var tsColName = metadataCol.getName().getSimple();
    var sourceWatermark = isTransactional ? useTransactionSourceWatermark : useSourceWatermark;
    var watermarkDelay = isTransactional ? transactionWatermark : defaultWatermark;
    if (sourceWatermark) {
      var connector = connectorConfig.get(FlinkConnectorConfigWrapper.CONNECTOR_KEY);
      if (connector == null || !connector.endsWith("-safe")) {
        throw new NoLocationStatementParserException(
            "SOURCE_WATERMARK is only supported in 'kafka-safe' and 'upsert-kafka-safe' connectors, but found: "
                + connector);
      }
      tableBuilder.setSourceWatermark(tsColName);
    } else {
      tableBuilder.setWatermarkMillis(tsColName, watermarkDelay.toMillis());
    }
  }

  private KafkaNewTopic createNewTopic(Table table, KafkaNewTopicModel.Type type) {
    String messageSchema;
    if (format.startsWith("avro")) {
      messageSchema =
          AvroToRelDataTypeConverter.convert2Avro(table.valueType, List.of()).toString();
    } else {
      messageSchema = ""; // TODO: generate JSON schema
    }
    return new KafkaNewTopic(
        new KafkaNewTopicModel(
            table.topicName(),
            table.tableName(),
            table.logicalName(),
            table.format(),
            numPartitions,
            replicationFactor,
            type,
            table.messageKeys(),
            messageSchema,
            table.config()));
  }

  private static KafkaIncludedTopicModel toIncludedTopic(Table table) {
    return new KafkaIncludedTopicModel(
        table.topicName(),
        table.tableName(),
        table.logicalName(),
        table.standaloneInclude().orElseThrow());
  }

  public record Table(
      String topicName,
      String tableName,
      String logicalName,
      String format,
      List<String> messageKeys,
      RelDataType valueType,
      Map<String, String> config,
      Optional<String> standaloneInclude)
      implements MutationCreateTable {

    @Override
    public MutationCreateTable withValueType(RelDataType inputValueType) {
      return new Table(
          topicName,
          tableName,
          logicalName,
          format,
          messageKeys,
          inputValueType,
          config,
          standaloneInclude);
    }

    @Override
    public Map<String, String> getConfig() {
      return config;
    }
  }
}
