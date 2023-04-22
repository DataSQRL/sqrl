package com.datasqrl.config;

import com.datasqrl.io.tables.TableConfig;
import java.util.UUID;
import lombok.Value;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Value
public class FlinkSourceFactoryContext implements SourceFactoryContext {
  StreamExecutionEnvironment env;
  String flinkName;
  TableConfig tableConfig;
  UUID uuid;
}
