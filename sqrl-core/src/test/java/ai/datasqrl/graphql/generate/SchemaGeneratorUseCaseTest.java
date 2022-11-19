package ai.datasqrl.graphql.generate;

import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.util.TestScript;
import ai.datasqrl.util.data.Nutshop;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public class SchemaGeneratorUseCaseTest extends AbstractSchemaGeneratorTest {


  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    super.setup(testInfo);
  }

  @ParameterizedTest
  @ArgumentsSource(TestScript.AllScriptsProvider.class)
  public void fullScriptTest(TestScript script) {
    initialize(IntegrationTestSettings.getInMemory(), script.getRootPackageDirectory());
    snapshotTest(script.getScript());
  }

  @SneakyThrows
  protected void produceSchemaFile(TestScript script) {
    initialize(IntegrationTestSettings.getInMemory(), script.getRootPackageDirectory());
    String schema = generateSchema(script.getScript());
    String filename = script.getScriptPath().getFileName().toString();
    if (filename.endsWith(".sqrl")) filename = filename.substring(0,filename.length()-5);
    filename += ".schema.graphql";
    Path path = script.getScriptPath().getParent().resolve(filename);
    Files.writeString(path,schema);
  }

  @Test
  @Disabled
  public void writeSchemaFile() {
    produceSchemaFile(Nutshop.INSTANCE.getScripts().get(1));
  }

}