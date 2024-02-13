package com.datasqrl.io;

import com.datasqrl.config.FlinkSinkFactoryContext;
import com.datasqrl.config.SinkFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.TableDescriptorSinkFactory;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.impl.kafka.KafkaDataSystemFactory;
import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.FormatDescriptor.Builder;
import org.apache.flink.table.api.TableDescriptor;

@AutoService(SinkFactory.class)
public class KafkaSinkFactory extends AbstractKafkaTableFactory
    implements TableDescriptorSinkFactory {

  @Override
  public String getSinkType() {
    return KafkaDataSystemFactory.SYSTEM_NAME;
  }

  @Override
  public TableDescriptor.Builder create(FlinkSinkFactoryContext context) {
    SqrlConfig connector = context.getTableConfig().getConnectorConfig();
    String topic = context.getTableConfig().getBase().getIdentifier();

    FormatFactory formatFactory = context.getFormatFactory();
    FormatDescriptor.Builder formatBuilder = FormatDescriptor.forFormat(formatFactory.getName());
    addOptions(formatBuilder, context.getTableConfig().getFormatConfig());
//    addFormatProps(formatBuilder, formatFactory.getAddlProps());
    TableDescriptor.Builder builder = TableDescriptor.forConnector("kafka")
        .option("topic", topic)
        .format(formatBuilder.build());

    addOptions(builder, connector);
    return builder;
  }
}
