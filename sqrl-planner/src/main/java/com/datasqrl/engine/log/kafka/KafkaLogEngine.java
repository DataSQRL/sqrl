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

import com.datasqrl.config.ConnectorConf;
import com.datasqrl.config.ConnectorConf.Context;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.config.TestRunnerConfiguration;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.flink.json.FlexibleJsonFlinkFormatTypeMapper;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.flinkrunner.format.json.FlexibleJsonFormat;
import com.datasqrl.graphql.server.MutationInsertType;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan.Query;
import com.datasqrl.planner.tables.FlinkConnectorConfig;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.util.TimeUtils;

@Slf4j
public class KafkaLogEngine extends ExecutionEngine.Base implements LogEngine {

  public static final String CONNECTOR_TOPIC_KEY = "topic";
  public static final String UPSERT_FORMAT = "upsert-%s";

  public static final String DEFAULT_TTL_KEY = "retention";

  public static final EnumSet<EngineFeature> KAFKA_FEATURES = EnumSet.of(EngineFeature.MUTATIONS);

  @Getter private final EngineConfig engineConfig;
  private final TestRunnerConfiguration testRunnerConfig;
  private final ConnectorConf streamConnectorConf;
  private final ConnectorConf mutationConnectorConf;
  private final Optional<Duration> defaultTTL;

  @Inject
  public KafkaLogEngine(PackageJson json, ConnectorFactoryFactory connectorFactory) {
    super(KafkaLogEngineFactory.ENGINE_NAME, EngineType.LOG, KAFKA_FEATURES);
    this.engineConfig = json.getEngines().getEngineConfigOrEmpty(KafkaLogEngineFactory.ENGINE_NAME);
    this.testRunnerConfig = json.getTestConfig();
    this.streamConnectorConf = connectorFactory.getConfig(KafkaLogEngineFactory.ENGINE_NAME);
    this.mutationConnectorConf =
        connectorFactory.getConfig(KafkaLogEngineFactory.ENGINE_NAME + "-mutation");
    defaultTTL = engineConfig.getSettingOptional(DEFAULT_TTL_KEY).map(TimeUtils::parseDuration);
  }

  @Override
  public EngineCreateTable createMutation(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      MutationInsertType insertType,
      Optional<Duration> ttl) {
    return createInternal(
        stage,
        originalTableName,
        tableBuilder,
        relDataType,
        Optional.empty(),
        ttl,
        true,
        insertType == MutationInsertType.TRANSACTION);
  }

  @Override
  public EngineCreateTable createTable(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      Optional<TableAnalysis> tableAnalysis) {
    return createInternal(
        stage,
        originalTableName,
        tableBuilder,
        relDataType,
        tableAnalysis,
        defaultTTL,
        false,
        false);
  }

  public EngineCreateTable createInternal(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      Optional<TableAnalysis> tableAnalysis,
      Optional<Duration> ttl,
      boolean isMutation,
      boolean isTransactional) {
    var ctxBuilder =
        Context.builder().tableName(originalTableName).tableId(tableBuilder.getTableName());
    var conf = isMutation ? mutationConnectorConf : streamConnectorConf;
    boolean isUpsert = false;
    List<String> messageKey = List.of();
    Map<String, String> topicConfig = new HashMap<>();
    if (tableBuilder.hasPartition()) {
      messageKey = tableBuilder.getPartition();
    }
    if (tableBuilder.hasPrimaryKey()) {
      if (tableAnalysis.map(TableAnalysis::getType).orElse(TableType.STATE).isState()) {
        isUpsert = true;
        // The primary key must be the partition key
        messageKey = List.of();
      } else {
        tableBuilder.removePrimaryKey();
      }
    }
    if (isMutation) {
      // Set watermark column for mutations based on 'timestamp' metadata
      for (SqlNode node : tableBuilder.getColumnList().getList()) {
        if (node instanceof SqlTableColumn.SqlMetadataColumn metadataColumn) {
          if (metadataColumn
              .getMetadataAlias()
              .filter(s -> s.equalsIgnoreCase("timestamp"))
              .isPresent()) {
            // TODO: make watermark configurable to 1 milli
            tableBuilder.setWatermarkMillis(metadataColumn.getName().getSimple(), 0);
          }
        }
      }
    }
    Map<String, String> connectorConfig = conf.toMapWithSubstitution(ctxBuilder.build());
    // Configure format depending on type
    String format = connectorConfig.get(FlinkConnectorConfig.FORMAT_KEY);
    Preconditions.checkArgument(
        format != null && !format.isBlank(),
        "Need to configure a 'format' for connector {}",
        KafkaLogEngineFactory.ENGINE_NAME);

    if (!messageKey.isEmpty()) {
      connectorConfig.put("key.fields", String.join(";", messageKey));
    }
    if (isUpsert) {
      connectorConfig.put(
          FlinkConnectorConfig.CONNECTOR_KEY,
          UPSERT_FORMAT.formatted(connectorConfig.get(FlinkConnectorConfig.CONNECTOR_KEY)));
    }
    if (!messageKey.isEmpty() || isUpsert) {
      connectorConfig.remove(FlinkConnectorConfig.FORMAT_KEY);
      connectorConfig.put(FlinkConnectorConfig.KEY_FORMAT_KEY, format);
      connectorConfig.put(FlinkConnectorConfig.VALUE_FORMAT_KEY, format);
      // Extract format-specific options and re-assign them with key. and value. prefixes
      Map<String, String> formatOptions = new HashMap<>();
      for (java.util.Iterator<Map.Entry<String, String>> it = connectorConfig.entrySet().iterator();
          it.hasNext(); ) {
        Map.Entry<String, String> entry = it.next();
        if (entry.getKey().startsWith(format)) {
          formatOptions.put(entry.getKey(), entry.getValue());
          it.remove();
        }
      }
      for (Map.Entry<String, String> entry : formatOptions.entrySet()) {
        connectorConfig.put("value." + entry.getKey(), entry.getValue());
        connectorConfig.put("key." + entry.getKey(), entry.getValue());
      }
    }

    if (isTransactional) {
      Preconditions.checkArgument(
          isMutation, "Only mutations can be used for transactions: %s", tableBuilder);
      connectorConfig.put("properties.isolation.level", "read_committed");
    }
    ttl.ifPresent(duration -> topicConfig.put("retention.ms", String.valueOf(duration.toMillis())));

    tableBuilder.setConnectorOptions(connectorConfig);
    String topicName = connectorConfig.get(CONNECTOR_TOPIC_KEY);
    // TODO: Add schema based on reldatatype
    return new NewTopic(topicName, tableBuilder.getTableName(), format, topicConfig);
  }

  @Override
  public DataTypeMapping getTypeMapping() {
    var format = String.valueOf(streamConnectorConf.toMap().get(FlinkConnectorConfig.FORMAT_KEY));
    if (FlexibleJsonFormat.FORMAT_NAME.equalsIgnoreCase(format)) {
      return new FlexibleJsonFlinkFormatTypeMapper();
    } else {
      log.error("Unexpected format for Kafka log engine: {}", format);
      return DataTypeMapping.NONE;
    }
  }

  @Override
  public EnginePhysicalPlan plan(MaterializationStagePlan stagePlan) {
    Map<String, String> table2TopicMap =
        StreamUtil.filterByClass(stagePlan.getTables(), NewTopic.class)
            .collect(Collectors.toMap(NewTopic::getTableName, NewTopic::getTopicName));
    // Plan queries
    for (Query query : stagePlan.getQueries()) {
      var errors = query.errors();
      var relNode = query.relNode();
      Map<String, Integer> filterColumns = new HashMap<>();
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
        for (RexNode condition : conditions) {
          checkErrors.accept(condition instanceof RexCall);
          var call = (RexCall) condition;
          checkErrors.accept(call.getOperator().getKind() == SqlKind.EQUALS);
          for (var i = 0; i < 2; i++) {
            if (call.getOperands().get(i) instanceof RexDynamicParam) {
              var argumentIndex = ((RexDynamicParam) call.getOperands().get(i)).getIndex();
              var colIdx = CalciteUtil.getNonAlteredInputRef(call.getOperands().get((i + 1) % 2));
              checkErrors.accept(colIdx.isPresent());
              filterColumns.put(fieldNames.get(colIdx.get()), argumentIndex);
            }
          }
        }
        checkErrors.accept(filterColumns.size() == conditions.size());
      }
      errors.checkFatal(
          relNode instanceof TableScan,
          "The Kafka engine currently only supports"
              + "simple filter queries without any transformations, but got: %s",
          relNode.explain());
      errors.checkFatal(
          !query.function().isPassthrough(),
          "Kafka does not support passthrough queries: %s",
          query.function());
      RelOptTable table = relNode.getTable();
      var tableName = table.getQualifiedName().get(2);
      var topicName = table2TopicMap.get(tableName);
      Preconditions.checkArgument(
          topicName != null, "Could not find topic for table: %s [%s]", tableName, table2TopicMap);
      query
          .function()
          .setExecutableQuery(new KafkaQuery(stagePlan.getStage(), topicName, filterColumns));
    }
    // Plan topic creation
    var topics =
        Streams.concat(stagePlan.getTables().stream(), stagePlan.getMutations().stream())
            .map(NewTopic.class::cast)
            .toList();

    var testRunnerTopics =
        testRunnerConfig.getCreateTopics().stream()
            .map(topicName -> new NewTopic(topicName, topicName))
            .toList();

    return new KafkaPhysicalPlan(topics, testRunnerTopics);
  }
}
