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
    context.getTableConfig().getConfig().asString("");
    SqrlConfig config = context.getTableConfig().getConnectorConfig();
    System.out.println();

    TableDescriptor.Builder builder = TableDescriptor.forConnector("kafka")
        .option("topic", config.asString("topic").get())
        .option("properties.bootstrap.servers", config.asString("bootstrap.servers").get())
        .format(context.getFormatFactory().getName());

    config.asString("scan.startup.mode").getOptional()
        .ifPresent(c->builder.option("scan.startup.mode", c));
    config.asString("scan.startup.specific-offsets").getOptional()
        .ifPresent(c->builder.option("scan.startup.specific-offsets", c));
    config.asString("scan.startup.timestamp-millis").getOptional()
        .ifPresent(c->builder.option("scan.startup.timestamp-millis", c));
    config.asString("sink.partitioner").getOptional()
        .ifPresent(c->builder.option("sink.partitioner", c));

    return builder;
  }
}
