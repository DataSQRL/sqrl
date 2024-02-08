package com.datasqrl.config;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.tables.TableConfig;
import java.util.UUID;
import lombok.Value;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Value
public class FlinkSourceFactoryContext implements SourceFactoryContext {
  @Deprecated
  StreamExecutionEnvironment env;
  String flinkName;
  TableConfig.Serialized tableConfig;
  FormatFactory formatFactory;
  UUID uuid;

  public TableConfig getTableConfig() {
    return tableConfig.deserialize(ErrorCollector.root());
  }
}
