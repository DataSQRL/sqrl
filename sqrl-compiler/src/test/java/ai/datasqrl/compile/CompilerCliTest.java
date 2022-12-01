package ai.datasqrl.compile;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.io.Resources;
import java.nio.file.Path;
import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class CompilerCliTest {

  @SneakyThrows
  @Test
  public void readConfig() {
    CompilerCli compilerCli = new CompilerCli();
    Path path = Path.of(Resources.getResource("c360bundle").toURI());

//    PackagerConfig config = compilerCli.readConfig(path, Optional.of("dev"));
//    assertEquals("dev", config.getDependencies().getDependencies()
//        .get("datasqrl.examples.Shared").getVariant());
//    assertEquals("dev", config.getEngines().getJdbc()
//        .getDatabase());
  }

}