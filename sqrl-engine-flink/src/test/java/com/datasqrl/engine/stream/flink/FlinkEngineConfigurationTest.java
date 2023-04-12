package com.datasqrl.engine.stream.flink;


import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import java.nio.charset.Charset;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class FlinkEngineConfigurationTest {

  @SneakyThrows
  @Test
  public void testPackageSerialization() {

    FlinkEngineConfiguration flinkEngineConfiguration =
        SqrlObjectMapper.INSTANCE.readValue(Resources.getResource("package-flink-conf.json"),
            FlinkEngineConfiguration.class);

    assertEquals(1, flinkEngineConfiguration.getFlinkConf().size());
  }
}