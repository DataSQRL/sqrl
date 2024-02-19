package com.datasqrl.config;

import com.datasqrl.io.formats.FormatFactory;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.FormatDescriptor.Builder;
import org.apache.flink.table.api.TableDescriptor;

public interface TableDescriptorSinkFactory extends SinkFactory<TableDescriptor.Builder, FlinkSinkFactoryContext> {

  @Override
  default String getEngine() {
    return "flink";
  }

  default FormatDescriptor.Builder createFormat(FormatFactory formatFactory,
      SqrlConfig formatConfig) {
    Builder builder = FormatDescriptor.forFormat(formatFactory.getTableApiSerializerName());
    addFormatOptions(builder, formatConfig);
    return builder;
  }

  default void addFormatOptions(FormatDescriptor.Builder formatBuilder, SqrlConfig formatConfig) {
    for (String key: formatConfig.getKeys()) {
      if (key.equals(FormatFactory.FORMAT_NAME_KEY)) continue;
      formatBuilder.option(key, formatConfig.asString(key).get());
    }
  }
}