/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import static com.datasqrl.packager.config.ScriptConfiguration.GRAPHQL_NORMALIZED_FILE_NAME;

import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.util.StringUtil;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.TestScript.PhysicalUseCaseProvider;
import com.datasqrl.util.data.Books;
import com.datasqrl.util.data.Sensors;
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
public class SchemaGeneratorUseCaseTest extends AbstractSchemaGeneratorTest {


  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    super.setup(testInfo);
  }

  @ParameterizedTest
  @ArgumentsSource(PhysicalUseCaseProvider.class)
  public void fullScriptTest(TestScript script) {
    initialize(IntegrationTestSettings.getInMemory(), script.getRootPackageDirectory());
    snapshotTest(script.getScript());
  }

  @SneakyThrows
  protected String produceSchemaString(TestScript script) {
    initialize(IntegrationTestSettings.getInMemory(), script.getRootPackageDirectory());
    return generateSchema(script.getScript());
  }

  @Test
  @Disabled
  public void writeSchemaFile() {
    TestScript script = Books.INSTANCE.getScripts().get(0);
    String schema = produceSchemaString(script);
    writeSchemaFile(script, schema);
  }

  @SneakyThrows
  private void writeSchemaFile(TestScript script, String schema) {
    String filename = script.getScriptPath().getFileName().toString();
    if (filename.endsWith(".sqrl")) {
      filename = StringUtil.removeFromEnd(filename, ".sqrl");
    }
    filename += "." + GRAPHQL_NORMALIZED_FILE_NAME;
    Path path = script.getScriptPath().getParent().resolve(filename);
    Files.writeString(path, schema);
  }

}