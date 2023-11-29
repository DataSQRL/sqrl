package com.datasqrl.io;

import com.datasqrl.config.FlinkSourceFactoryContext;
import com.datasqrl.config.TableDescriptorSourceFactory;
import com.datasqrl.io.impl.kafka.KafkaDataSystemFactory;
import org.apache.flink.table.api.TableDescriptor.Builder;

public class KafkaTableSourceFactory implements TableDescriptorSourceFactory {

  @Override
  public String getSourceName() {
    return KafkaDataSystemFactory.SYSTEM_NAME;
  }

  @Override
  public Builder create(FlinkSourceFactoryContext context) {
    return null;
  }
}
