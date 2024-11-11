package com.datasqrl.tests;

import static junit.framework.TestCase.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;

public class IcebergTestExtension extends DuckdbTestExtension {

  @Override
  public void setup() {
    super.setup();
  }

  @Override
  public void teardown() {
    //assert that there is a 'my-table' iceberg table
    assertTrue(Files.exists(Path.of("/tmp/duckdb/default_database/my-table/metadata/v1.metadata.json")));
    super.teardown();
  }
}