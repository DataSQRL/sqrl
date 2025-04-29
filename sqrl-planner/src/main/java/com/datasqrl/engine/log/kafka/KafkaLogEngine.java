package com.datasqrl.engine.log.kafka;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.ConnectorConf;
import com.datasqrl.config.ConnectorConf.Context;
import com.datasqrl.config.ConnectorFactory;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.flink.avro.AvroFlinkFormatTypeMapper;
import com.datasqrl.datatype.flink.json.FlexibleJsonFlinkFormatTypeMapper;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.log.LogFactory;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.flinkrunner.format.json.FlexibleJsonFormat;
import com.datasqrl.io.schema.avro.AvroTableSchemaFactory;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.global.PhysicalDAGPlan.LogStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.StreamUtil;
import com.datasqrl.v2.analyzer.TableAnalysis;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan.Query;
import com.datasqrl.v2.tables.FlinkConnectorConfig;
import com.datasqrl.v2.tables.FlinkTableBuilder;
import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import com.google.inject.Inject;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaLogEngine extends ExecutionEngine.Base implements LogEngine {

  @Getter
  private final EngineConfig engineConfig;
  @Deprecated
  private final ConnectorFactory connectorFactory;

  private final ConnectorConf streamConnectorConf;
  private final ConnectorConf upsertConnectorConf;
  private final ConnectorConf keyedStreamConnectorConf;

  @Inject
  public KafkaLogEngine(PackageJson json,
      ConnectorFactoryFactory connectorFactory) {
    super(KafkaLogEngineFactory.ENGINE_NAME, EngineType.LOG, EngineFeature.STANDARD_LOG);
    this.engineConfig = json.getEngines().getEngineConfig(KafkaLogEngineFactory.ENGINE_NAME)
        .orElseGet(() -> new EmptyEngineConfig(KafkaLogEngineFactory.ENGINE_NAME));
    this.connectorFactory = connectorFactory.create(EngineType.LOG, "kafka")
        .orElseThrow(()->new RuntimeException("Could not find kafka connector"));
    this.streamConnectorConf = connectorFactory.getConfig(KafkaLogEngineFactory.ENGINE_NAME);
    this.upsertConnectorConf = connectorFactory.getConfig(KafkaLogEngineFactory.ENGINE_NAME+"-upsert");
    this.keyedStreamConnectorConf = connectorFactory.getConfig(KafkaLogEngineFactory.ENGINE_NAME+"-keyed");
  }

  @Override
  public EngineCreateTable createTable(ExecutionStage stage, String originalTableName,
      FlinkTableBuilder tableBuilder, RelDataType relDataType, Optional<TableAnalysis> tableAnalysis) {
    var ctxBuilder = Context.builder()
        .tableName(tableBuilder.getTableName())
        .origTableName(originalTableName);
    var conf = streamConnectorConf;
    List<String> messageKey = List.of();
    if (tableBuilder.hasPartition()) {
      messageKey = tableBuilder.getPartition();
    }
    if (tableBuilder.hasPrimaryKey()) {
      if (tableAnalysis.map(TableAnalysis::getType).orElse(TableType.STATE).isState()) {
        conf = upsertConnectorConf;
        //The primary key must be the partition key
        messageKey = List.of();
      } else {
        tableBuilder.removePrimaryKey();
      }
    }
    if (!messageKey.isEmpty()) {
      conf = keyedStreamConnectorConf;
      ctxBuilder.variable("kafka-key", String.join(";", messageKey));
    }
    /* TODO: add engine configuration option that makes rowtime (if present in relDataType) the kafka timestamp by default by
        annotating the column in the table with 'timestamp' metadata in the column list of the table builder
     */
    //Set watermark column for mutations based on 'timestamp' metadata
    for (SqlNode node : tableBuilder.getColumnList().getList()) {
      if (node instanceof SqlTableColumn.SqlMetadataColumn metadataColumn) {
        if (metadataColumn.getMetadataAlias().filter(s -> s.equalsIgnoreCase("timestamp")).isPresent()) {
          //TODO: make watermark configurable to 1 milli
          tableBuilder.setWatermarkMillis(metadataColumn.getName().getSimple(), 0);
        }
      }
    }
    tableBuilder.setConnectorOptions(conf.toMapWithSubstitution(ctxBuilder.build()));
    return new NewTopic(originalTableName, tableBuilder.getTableName());
  }

  @Override
  public DataTypeMapping getTypeMapping() {
    var format = String.valueOf(streamConnectorConf.toMap().get(FlinkConnectorConfig.FORMAT_KEY));
    if (FlexibleJsonFormat.FORMAT_NAME.equalsIgnoreCase(format)) {
      return new FlexibleJsonFlinkFormatTypeMapper();
    } else if (AvroTableSchemaFactory.SCHEMA_TYPE.equalsIgnoreCase(format)) {
      return new AvroFlinkFormatTypeMapper();
    } else {
      log.error("Unexpected format for Kafka log engine: {}", format);
      return DataTypeMapping.NONE;
    }
  }

  @Override
  public EnginePhysicalPlan plan(MaterializationStagePlan stagePlan) {
    Map<String, String> table2TopicMap = StreamUtil.filterByClass(stagePlan.getTables(), NewTopic.class)
        .collect(Collectors.toMap(NewTopic::getTableName, NewTopic::getTopicName));
    //Plan queries
    for (Query query : stagePlan.getQueries()) {
      var errors = query.getErrors();
      var relNode = query.getRelNode();
      Map<String,Integer> filterColumns = new HashMap<>();
      if (relNode instanceof Project project && relNode.getRowType().equals(project.getInput().getRowType())) {
        relNode = project.getInput();
      }
      if (relNode instanceof Filter filter) {
        Consumer<Boolean> checkErrors = b -> errors.checkFatal(b, "Expected simple equality condition for Kafka filter: %s", filter.getCondition());
        relNode = filter.getInput();
        /*We expect that the filter is a simple AND condition of equality conditions of the sort `column = ?`
          i.e. a RexDynamicParam on one side and a RexInputRef on the other
         */
        var conditions = stagePlan.getUtils().getRexUtil().getConjunctions(filter.getCondition());
        var fieldNames = filter.getRowType().getFieldNames();
        for (RexNode condition : conditions) {
          checkErrors.accept(condition instanceof RexCall);
          var call = (RexCall) condition;
          checkErrors.accept(call.getOperator().getKind()==SqlKind.EQUALS);
          for (var i = 0; i < 2; i++) {
            if (call.getOperands().get(i) instanceof RexDynamicParam) {
              var argumentIndex = ((RexDynamicParam) call.getOperands().get(i)).getIndex();
              var colIdx = CalciteUtil.getNonAlteredInputRef(call.getOperands().get((i+1)%2));
              checkErrors.accept(colIdx.isPresent());
              filterColumns.put(fieldNames.get(colIdx.get()), argumentIndex);
            }
          }
        }
        checkErrors.accept(filterColumns.size()== conditions.size());
      }
      errors.checkFatal(relNode instanceof TableScan, "The Kafka engine currently only supports"
          + "simple filter queries without any transformations, but got: %s", relNode.explain());
      RelOptTable table = relNode.getTable();
      var tableName = table.getQualifiedName().get(2);
      var topicName = table2TopicMap.get(tableName);
      Preconditions.checkArgument(topicName!=null, "Could not find topic for table: %s [%s]", tableName, table2TopicMap);
      query.getFunction().setExecutableQuery(new KafkaQuery(stagePlan.getStage(), topicName, filterColumns));
    }
    //Plan topic creation
    return new KafkaPhysicalPlan(
        Streams.concat(stagePlan.getTables().stream(),
            stagePlan.getMutations().stream())
            .map(NewTopic.class::cast).collect(Collectors.toList()));
  }

  @Override
  @Deprecated
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      List<StagePlan> stagePlans, SqrlFramework framework, ErrorCollector errorCollector) {
    Preconditions.checkArgument(plan instanceof LogStagePlan);

    List<NewTopic> logTopics = ((LogStagePlan) plan).getLogs().stream()
        .map(log -> (KafkaTopic) log)
        .map(KafkaTopic::getTopicName)
        .map(name -> new NewTopic(name,name))
        .collect(Collectors.toList());

    return new KafkaPhysicalPlan(logTopics);
  }

  @Override
  public LogFactory getLogFactory() {
    return new KafkaLogFactory(connectorFactory);
  }
}
