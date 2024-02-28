package com.datasqrl.discovery;


import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigUtil;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.stream.flink.ExecutionEnvironmentFactory;
import com.datasqrl.engine.stream.flink.FlinkStreamBuilder;
import com.datasqrl.engine.stream.flink.LocalFlinkStreamEngineImpl;
import java.util.Map;
import lombok.NonNull;

public class FlinkJobFactory {

  public FlinkStreamBuilder createFlinkJob(SqrlConfig config) {
    return new FlinkStreamBuilder(this,
        new ExecutionEnvironmentFactory(getFlinkConfiguration(config))
            .createEnvironment());
  }

  public static Map<String,String> getFlinkConfiguration(@NonNull SqrlConfig config) {
    return SqrlConfigUtil.toStringMap(config, EngineFactory.getReservedKeys());
  }


}
