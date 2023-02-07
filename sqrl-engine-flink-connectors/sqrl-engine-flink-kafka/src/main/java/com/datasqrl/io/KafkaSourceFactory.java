package com.datasqrl.io;

import com.datasqrl.config.SourceFactory;
import com.datasqrl.config.SourceServiceLoader.SourceFactoryContext;
import com.datasqrl.engine.stream.flink.FlinkSourceFactoryContext;
import com.datasqrl.io.impl.kafka.KafkaDataSystem;
import com.datasqrl.io.impl.kafka.KafkaDataSystemConfig;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class KafkaSourceFactory implements
    SourceFactory<SingleOutputStreamOperator<TimeAnnotatedRecord<String>>> {

  @Override
  public String getEngine() {
    return "flink";
  }

  @Override
  public String getSourceName() {
    return KafkaDataSystemConfig.SYSTEM_TYPE;
  }

  @Override
  public SingleOutputStreamOperator<TimeAnnotatedRecord<String>> create(DataSystemConnector connector, SourceFactoryContext context) {
    FlinkSourceFactoryContext ctx = (FlinkSourceFactoryContext) context;

    KafkaDataSystem.Connector kafkaSource = (KafkaDataSystem.Connector) connector;
    String topic = kafkaSource.getTopicPrefix() + ctx.getTable().getConfiguration().getIdentifier();
    String groupId = ctx.getFlinkName() + "-" + ctx.getUuid();

    KafkaSourceBuilder<TimeAnnotatedRecord<String>> builder = org.apache.flink.connector.kafka.source.KafkaSource.<TimeAnnotatedRecord<String>>builder()
        .setBootstrapServers(kafkaSource.getServersAsString())
        .setTopics(topic)
        .setStartingOffsets(OffsetsInitializer.earliest()) //TODO: work with commits
        .setGroupId(groupId);

    builder.setDeserializer(
        new KafkaTimeValueDeserializationSchemaWrapper<>(new SimpleStringSchema()));

    return ctx.getEnv().fromSource(builder.build(),
        WatermarkStrategy.noWatermarks(), ctx.getFlinkName());
  }
}
