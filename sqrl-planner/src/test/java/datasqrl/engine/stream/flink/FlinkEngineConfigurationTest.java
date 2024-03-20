package datasqrl.engine.stream.flink;


import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigUtil;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.error.ErrorCollector;
import com.google.common.io.Resources;
import java.util.Map;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class FlinkEngineConfigurationTest {

  @SneakyThrows
  @Test
  @Disabled
  public void testPackageSerialization() {
    SqrlConfig config = SqrlConfigCommons.fromURL(ErrorCollector.root(), Resources.getResource("package-flink-conf.json"));
    Map<String,String> flinkConf = getFlinkConfiguration(config);
    assertEquals(2, flinkConf.size());
  }

  public static Map<String,String> getFlinkConfiguration(@NonNull SqrlConfig config) {
    return SqrlConfigUtil.toStringMap(config, EngineFactory.getReservedKeys());
  }
}