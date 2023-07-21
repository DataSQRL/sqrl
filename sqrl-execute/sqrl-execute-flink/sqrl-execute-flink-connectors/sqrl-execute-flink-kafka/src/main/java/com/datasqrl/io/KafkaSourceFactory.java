package com.datasqrl.io;

import com.datasqrl.config.DataStreamSourceFactory;
import com.datasqrl.config.FlinkSourceFactoryContext;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.impl.kafka.KafkaDataSystemFactory;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.datasqrl.timestamp.ProgressingMonotonicEventTimeWatermarks;
import java.time.Clock;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class KafkaSourceFactory implements DataStreamSourceFactory {

  @Override
  public String getSourceName() {
    return KafkaDataSystemFactory.SYSTEM_NAME;
  }

  @Override
  public SingleOutputStreamOperator<TimeAnnotatedRecord<String>> create(FlinkSourceFactoryContext ctx) {
    TableConfig tableConfig = ctx.getTableConfig();
    SqrlConfig connector = tableConfig.getConnectorConfig();
    String topic = tableConfig.getBase().getIdentifier();
    String groupId = ctx.getFlinkName() + "-" + ctx.getUuid();

    KafkaSourceBuilder<TimeAnnotatedRecord<String>> builder = org.apache.flink.connector.kafka.source.KafkaSource.<TimeAnnotatedRecord<String>>builder()
        .setBootstrapServers(connector.asString(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).get())
        .setTopics(topic)
        .setStartingOffsets(OffsetsInitializer.earliest()) //TODO: work with commits
        .setGroupId(groupId);

    connector.asString("security.protocol").getOptional()
            .ifPresent(c->builder.setProperty("security.protocol", c));
    connector.asString("sasl.mechanism").getOptional()
            .ifPresent(c->builder.setProperty("sasl.mechanism", c));
    connector.asString("group.id").getOptional()
            .ifPresent(c->builder.setProperty("group.id", c));
    connector.asString("sasl.jaas.config").getOptional()
            .ifPresent(c->builder.setProperty("sasl.jaas.config", c));
    connector.asString("sasl.client.callback.handler.class").getOptional()
            .ifPresent(c->builder.setProperty("sasl.client.callback.handler.class", c));

    builder.setDeserializer(
        new KafkaTimeValueDeserializationSchemaWrapper<>(new SimpleStringSchema()));

    return ctx.getEnv().fromSource(builder.build(),
        ProgressingMonotonicEventTimeWatermarks.<TimeAnnotatedRecord<String>>of(Duration.ofMillis(1000))
            .withTimestampAssigner((record, timestamp) -> record.getSourceTime().toEpochMilli())
            .withIdleness(Duration.ofMillis(5000)),
        ctx.getFlinkName());
  }
}
