package com.datasqrl.io;

import com.datasqrl.config.FlinkSinkFactoryContext;
import com.datasqrl.config.SinkFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.TableDescriptorSinkFactory;
import com.datasqrl.io.impl.kafka.KafkaDataSystemFactory;
import com.google.auto.service.AutoService;
import org.apache.flink.table.api.TableDescriptor;

@AutoService(SinkFactory.class)
public class KafkaSinkFactory
    implements TableDescriptorSinkFactory {

  @Override
  public String getSinkType() {
    return KafkaDataSystemFactory.SYSTEM_NAME;
  }

  @Override
  public TableDescriptor.Builder create(FlinkSinkFactoryContext context) {
    SqrlConfig connector = context.getTableConfig().getConnectorConfig();
    String topic = context.getTableConfig().getBase().getIdentifier();


    TableDescriptor.Builder builder = TableDescriptor.forConnector("kafka")
        .option("topic", topic)
        .option("properties.bootstrap.servers", connector.asString("bootstrap.servers").get())
        .format(context.getFormatFactory().getName());

    connector.asString("scan.startup.mode").getOptional()
        .ifPresent(c->builder.option("scan.startup.mode", c));
    connector.asString("scan.startup.specific-offsets").getOptional()
        .ifPresent(c->builder.option("scan.startup.specific-offsets", c));
    connector.asString("scan.startup.timestamp-millis").getOptional()
        .ifPresent(c->builder.option("scan.startup.timestamp-millis", c));
    connector.asString("sink.partitioner").getOptional()
        .ifPresent(c->builder.option("sink.partitioner", c));

    return builder;
  }
}
