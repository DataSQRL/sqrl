package com.datasqrl.io;

import com.datasqrl.config.FlinkSourceFactoryContext;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.TableDescriptorSourceFactory;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.impl.kafka.KafkaDataSystemFactory;
import java.util.Optional;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableDescriptor.Builder;

public class KafkaTableSourceFactory extends AbstractKafkaTableFactory implements TableDescriptorSourceFactory {

  @Override
  public String getSourceName() {
    return KafkaDataSystemFactory.SYSTEM_NAME;
  }

  @Override
  public Builder create(FlinkSourceFactoryContext context) {
    SqrlConfig connector = context.getTableConfig().getConnectorConfig();
    String topic = context.getTableConfig().getBase().getIdentifier();

    FormatFactory formatFactory = context.getFormatFactory();
    FormatDescriptor.Builder formatBuilder = FormatDescriptor.forFormat(formatFactory.getName());
    addOptions(formatBuilder, context.getTableConfig().getFormatConfig());

    TableDescriptor.Builder builder = TableDescriptor.forConnector("kafka")
        .option("topic", topic)
        .format(formatBuilder.build());

    addOptions(builder, connector);
    return builder;
  }

  @Override
  public Optional<String> getSourceTimeMetaData() {
    return Optional.of("timestamp");
  }
}
