package com.datasqrl.io;

import com.datasqrl.config.DataStreamSourceFactory;
import com.datasqrl.config.SourceFactory;
import com.datasqrl.config.SourceFactoryContext;
import com.datasqrl.config.FlinkSourceFactoryContext;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.impl.kafka.KafkaDataSystemConfig;
import com.datasqrl.io.impl.kafka.KafkaDataSystemConnector;
import com.datasqrl.io.impl.kafka.KafkaDataSystemFactory;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.util.TimeAnnotatedRecord;
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
    SqrlConfig connectorConfig = tableConfig.getConnectorConfig();
    String topic = tableConfig.getBase().getIdentifier();
    String groupId = ctx.getFlinkName() + "-" + ctx.getUuid();

    KafkaSourceBuilder<TimeAnnotatedRecord<String>> builder = org.apache.flink.connector.kafka.source.KafkaSource.<TimeAnnotatedRecord<String>>builder()
        .setBootstrapServers(connectorConfig.asString(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).get())
        .setTopics(topic)
        .setStartingOffsets(OffsetsInitializer.earliest()) //TODO: work with commits
        .setGroupId(groupId);

    builder.setDeserializer(
        new KafkaTimeValueDeserializationSchemaWrapper<>(new SimpleStringSchema()));

    return ctx.getEnv().fromSource(builder.build(),
        WatermarkStrategy.noWatermarks(), ctx.getFlinkName());
  }
}
