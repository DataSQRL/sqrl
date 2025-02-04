package com.datasqrl.engine.log.kafka;


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
import com.datasqrl.format.FlexibleJsonFormat;
import com.datasqrl.io.schema.avro.AvroTableSchemaFactory;
import com.datasqrl.plan.global.PhysicalDAGPlan.LogStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan.Query;
import com.datasqrl.v2.tables.FlinkConnectorConfig;
import com.datasqrl.v2.tables.FlinkTableBuilder;
import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import java.util.ArrayList;
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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

@Slf4j
public class KafkaLogEngine extends ExecutionEngine.Base implements LogEngine {

  @Getter
  private final EngineConfig engineConfig;
  @Deprecated
  private final ConnectorFactory connectorFactory;

  private final ConnectorConf streamConnectorConf;
  private final ConnectorConf upsertConnectorConf;

  @Inject
  public KafkaLogEngine(PackageJson json,
      ConnectorFactoryFactory connectorFactory) {
    super(KafkaLogEngineFactory.ENGINE_NAME, EngineType.LOG, EnumSet.noneOf(EngineFeature.class));
    this.engineConfig = json.getEngines().getEngineConfig(KafkaLogEngineFactory.ENGINE_NAME)
        .orElseGet(() -> new EmptyEngineConfig(KafkaLogEngineFactory.ENGINE_NAME));
    this.connectorFactory = connectorFactory.create(EngineType.LOG, "kafka")
        .orElseThrow(()->new RuntimeException("Could not find kafka connector"));
    this.streamConnectorConf = connectorFactory.getConfig(KafkaLogEngineFactory.ENGINE_NAME);
    this.upsertConnectorConf = connectorFactory.getConfig(KafkaLogEngineFactory.ENGINE_NAME+"-upsert");
  }

  @Override
  public EngineCreateTable createTable(ExecutionStage stage, String originalTableName,
      FlinkTableBuilder tableBuilder, RelDataType relDataType) {
    String topicName = tableBuilder.getTableName();
    Context.ContextBuilder ctxBuilder = Context.builder()
        .tableName(topicName)
        .origTableName(originalTableName);
    ConnectorConf conf = streamConnectorConf;
    if (tableBuilder.hasPrimaryKey()) {
      conf = upsertConnectorConf;
      ctxBuilder.variable("kafka-key", String.join(";", tableBuilder.getPrimaryKey().get()));
    }
    /* TODO: add engine configuration option that makes rowtime (if present in relDataType) the kafka timestamp by default by
        annotating the column in the table with 'timestamp' metadata in the column list of the table builder
     */
    //Set watermark column for mutations based on 'timestamp' metadata
    for (SqlNode node : tableBuilder.getColumnList().getList()) {
      if (node instanceof SqlTableColumn.SqlMetadataColumn) {
        SqlTableColumn.SqlMetadataColumn metadataColumn = (SqlTableColumn.SqlMetadataColumn) node;
        if (metadataColumn.getMetadataAlias().filter(s -> s.equalsIgnoreCase("timestamp")).isPresent()) {
          //TODO: make watermark configurable to 1 milli
          tableBuilder.setWatermarkMillis(metadataColumn.getName().getSimple(), 0);
        }
      }
    }
    tableBuilder.setConnectorOptions(conf.toMapWithSubstitution(ctxBuilder.build()));
    return new NewTopic(topicName);
  }

  @Override
  public DataTypeMapping getTypeMapping() {
    String format = String.valueOf(streamConnectorConf.toMap().get(FlinkConnectorConfig.FORMAT_KEY));
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
    //Plan queries
    for (Query query : stagePlan.getQueries()) {
      ErrorCollector errors = query.getErrors();
      RelNode relNode = query.getRelNode();
      Map<String,Integer> filterColumns = new HashMap<>();
      if (relNode instanceof Filter) {
        Filter filter = (Filter) relNode;
        Consumer<Boolean> checkErrors = b -> errors.checkFatal(b, "Expected simple equality condition for Kafka filter: %s", filter.getCondition());
        relNode = filter.getInput();
        /*We expect that the filter is a simple AND condition of equality conditions of the sort `column = ?`
          i.e. a RexDynamicParam on one side and a RexInputRef on the other
         */
        List<RexNode> conditions = stagePlan.getUtils().getRexUtil().getConjunctions(filter.getCondition());
        List<String> fieldNames = filter.getRowType().getFieldNames();
        for (RexNode condition : conditions) {
          checkErrors.accept(conditions instanceof RexCall);
          RexCall call = (RexCall) condition;
          checkErrors.accept(call.getOperator().getKind()==SqlKind.EQUALS);
          for (int i = 0; i < 2; i++) {
            if (call.getOperands().get(i) instanceof RexDynamicParam) {
              int argumentIndex = ((RexDynamicParam) call.getOperands().get(i)).getIndex();
              Optional<Integer> colIdx = CalciteUtil.getNonAlteredInputRef(call.getOperands().get((i+1)%2));
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
      String topicName = ((TableSourceTable)table).contextResolvedTable().getIdentifier().getObjectName();
      query.getFunction().setExecutableQuery(new KafkaQuery(stagePlan.getStage(), topicName, filterColumns));
    }
    //Plan topic creation
    return new KafkaPhysicalPlan(
        Streams.concat(stagePlan.getTables().stream(),
            stagePlan.getMutations().stream())
            .map(NewTopic.class::cast).collect(Collectors.toList()));
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      List<StagePlan> stagePlans, SqrlFramework framework, ErrorCollector errorCollector) {
    Preconditions.checkArgument(plan instanceof LogStagePlan);

    List<NewTopic> logTopics = ((LogStagePlan) plan).getLogs().stream()
        .map(log -> (KafkaTopic) log)
        .map(KafkaTopic::getTopicName)
        .map(NewTopic::new)
        .collect(Collectors.toList());

    return new KafkaPhysicalPlan(logTopics);
  }

  @Override
  public LogFactory getLogFactory() {
    return new KafkaLogFactory(connectorFactory);
  }
}
