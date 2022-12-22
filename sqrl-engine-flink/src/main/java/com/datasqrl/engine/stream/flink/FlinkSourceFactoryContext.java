package com.datasqrl.engine.stream.flink;

import com.datasqrl.config.SourceServiceLoader.SourceFactoryContext;
import com.datasqrl.io.tables.TableInput;
import java.util.UUID;
import lombok.Value;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Value
public class FlinkSourceFactoryContext implements SourceFactoryContext {
  StreamExecutionEnvironment env;
  String flinkName;
  TableInput table;
  UUID uuid;
}
