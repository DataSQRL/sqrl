package com.datasqrl.engine.stream.flink;


import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.SqrlObjectMapper;
import com.google.common.io.Resources;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class FlinkEngineConfigurationTest {

  @SneakyThrows
  @Test
  public void testPackageSerialization() {
    SqrlConfig config = SqrlConfigCommons.fromURL(ErrorCollector.root(), Resources.getResource("package-flink-conf.json"));
    Map<String,String> flinkConf = new FlinkEngineFactory().getFlinkConfiguration(config);
    assertEquals(1, flinkConf.size());
  }
}