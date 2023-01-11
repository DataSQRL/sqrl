/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import com.datasqrl.AbstractPhysicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.data.Retail;
import com.datasqrl.util.data.Retail.RetailScriptNames;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class FlinkDebugPhysicalIT extends AbstractPhysicalSQRLIT {

  private Retail example = Retail.INSTANCE;
  private Path outputPath = example.getRootPackageDirectory().resolve("export-data");

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
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
  public void debugC3602OutputTest() {
    initialize(IntegrationTestSettings.getFlinkWithDBConfig()
          .debugger(DebuggerConfig.of(NamePath.of("output"),null))
        .build(),
        example.getRootPackageDirectory());
    TestScript script = example.getScript(RetailScriptNames.FULL);
    validateTables(script.getScript(),"favorite_categories", "order_stats",
        "NewCustomerPromotion", "order_again", "total");
    return;
  }

  @Test
  public void debugC3602OutputSelectTablesTest() {
    initialize(IntegrationTestSettings.getFlinkWithDBConfig()
            .debugger(DebuggerConfig.of(NamePath.of("output"),
                toName("order_stats", "NewCustomerPromotion", "order_again", "total")))
            .build(),
        example.getRootPackageDirectory());
    TestScript script = example.getScript(RetailScriptNames.FULL);
    validateTables(script.getScript(),"favorite_categories");
    return;
  }

  public static Set<Name> toName(String... tables) {
    Preconditions.checkArgument(tables!=null && tables.length>0);
    return Arrays.stream(tables).map(Name::system).collect(Collectors.toSet());
  }

}
