package com.datasqrl.io;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.impl.kafka.KafkaDataSystemFactory;
import java.util.List;
import java.util.Set;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.TableDescriptor;

public abstract class AbstractKafkaTableFactory {


  public static final String FLINK_PROPERTIES_PREFIX = "properties.";

  public static final List<String> FLINK_PREFIXES = List.of("scan.","sink.","key.","value.");

  protected void addOptions(TableDescriptor.Builder tblBuilder, SqrlConfig connectorConfig) {
    for (String key : connectorConfig.getKeys()) {
      if (KafkaDataSystemFactory.ALL_KEYS.contains(key)) continue; //Don't add DATASQRL configs
      if (FLINK_PREFIXES.stream().anyMatch(key::startsWith)) {
        //Flink config options are just forwarded
        tblBuilder.option(key,connectorConfig.asString(key).get());
      } else {
        //Kafka config options are prefixed
        tblBuilder.option(FLINK_PROPERTIES_PREFIX+key, connectorConfig.asString(key).get());
      }
    }
  }
}
