/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import com.datasqrl.AbstractPhysicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.data.Retail;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class FlinkInputErrorIT extends AbstractPhysicalSQRLIT {

  private Retail example = Retail.INSTANCE;
  private Path outputPath = example.getRootPackageDirectory().resolve("export-data");

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
    this.closeSnapshotOnValidate = false;
    if (!Files.isDirectory(outputPath)) {
      Files.createDirectory(outputPath);
    }
  }

  @AfterEach
  @SneakyThrows
  public void cleanupDirectory() {
    //Contents written to outputPath are validated in validateTables()
    if (Files.isDirectory(outputPath)) {
      FileUtils.deleteDirectory(outputPath.toFile());
    }
  }

  @Test
  public void brokenRetailDataTest() {
    initialize(IntegrationTestSettings.getFlinkWithDBConfig()
        .errorSink(NamePath.of("output","errors"))
        .build(),
        example.getRootPackageDirectory(),
        Optional.of(outputPath));
    validateTables("IMPORT ecommerce-broken.*;","customer", "orders",
        "product");
    Path errorPath = outputPath.resolve("errors");
//    List<String> errors = FileTestUtil.collectAllPartFilesByLine(errorPath);
//    String errorText = errors.stream().sorted().collect(Collectors.joining("\n"));
    //Ingesttime makes it non-deterministic
    String errorText = String.valueOf(FileTestUtil.countLinesInAllPartFiles(errorPath));

    snapshot.addContent(errorText,"input-errors");
    snapshot.createOrValidate();
  }

}
