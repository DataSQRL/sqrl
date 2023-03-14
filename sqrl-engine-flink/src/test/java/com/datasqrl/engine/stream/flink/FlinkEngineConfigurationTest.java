package com.datasqrl.engine.stream.flink;


import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import java.nio.charset.Charset;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class FlinkEngineConfigurationTest {

  @SneakyThrows
  @Test
  public void testPackageSerialization() {
    ObjectMapper objectMapper = new ObjectMapper();
    FlinkEngineConfiguration flinkEngineConfiguration =
        objectMapper.readValue(Resources.getResource("package-flink-conf.json"),
            FlinkEngineConfiguration.class);

    assertEquals(1, flinkEngineConfiguration.getFlinkConf().size());
  }
}