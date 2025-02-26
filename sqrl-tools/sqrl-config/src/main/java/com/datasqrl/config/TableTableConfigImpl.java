package com.datasqrl.config;

import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class TableTableConfigImpl implements TableConfig.TableTableConfig {

  SqrlConfig sqrlConfig;

  public static final String TYPE_KEY = "type";
  public static final String TIMESTAMP_COL_KEY = "timestamp";
  public static final String WATERMARK_KEY = "watermark-millis";
  public static String PRIMARYKEY_KEY = "primary-key";
  public static String PARTITIONKEY_KEY = "partition-key";

  public ExternalDataType getType() {
    return SqrlConfig.getEnum(
        sqrlConfig.asString(TYPE_KEY),
        ExternalDataType.class,
        Optional.of(ExternalDataType.source_and_sink));
  }

  public long getWatermarkMillis() {
    return sqrlConfig.asLong(WATERMARK_KEY).withDefault(-1L).get();
  }

  public Optional<String> getTimestampColumn() {
    return sqrlConfig.asString(TIMESTAMP_COL_KEY).getOptional();
  }

  public Optional<List<String>> getPrimaryKey() {
    return sqrlConfig.asList(PRIMARYKEY_KEY, String.class).getOptional();
  }

  public SqrlConfig getConfig() {
    return sqrlConfig;
  }

  public SqrlConfig.Value<List<String>> getPartitionKeys() {
    return sqrlConfig.asList(PARTITIONKEY_KEY, String.class);
  }

  public Optional<List<String>> getPartitionKey() {
    if (!sqrlConfig.hasKey(PARTITIONKEY_KEY)) {
      return Optional.empty();
    }
    return Optional.of(sqrlConfig.asList(PARTITIONKEY_KEY, String.class).get());
  }
}
