package com.datasqrl.graphql.generate;

import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.TestScript.Impl;
import com.datasqrl.util.data.Nutshop;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Slf4j
public class OpenLineageExampleTest extends AbstractSchemaGeneratorTest {


  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    super.setup(testInfo);
  }

  @Test
  public void fullScriptTest() {

  }


}
