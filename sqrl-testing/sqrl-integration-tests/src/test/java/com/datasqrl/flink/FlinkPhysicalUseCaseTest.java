/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import com.datasqrl.AbstractPhysicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.TestScript.PhysicalUseCaseProvider;
import com.datasqrl.util.data.Clickstream;
import com.datasqrl.util.data.Sensors;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Set;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class FlinkPhysicalUseCaseTest extends AbstractPhysicalSQRLIT {

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
  }

  @AfterEach
  public void tearDown() {
    super.tearDown();
  }

  @SneakyThrows
  private void scriptTest(TestScript script, boolean removeTimestamps, boolean snapshotData) {
    initialize(IntegrationTestSettings.getFlinkWithDB(), script.getRootPackageDirectory(), Optional.empty());
    validateTables(Files.readString(script.getScriptPath()), script.getResultTables(),
        removeTimestamps ? ImmutableSet.copyOf(script.getResultTables()) : Set.of(),
        snapshotData ? Set.of() : ImmutableSet.copyOf(script.getResultTables()));
  }

  @ParameterizedTest
  @ArgumentsSource(PhysicalUseCaseProvider.class)
  public void fullScriptTest(TestScript script) {
    scriptTest(script, true, script.dataSnapshot());
  }


  @Test
  @Disabled
  public void forDebuggingIndividualUseCases() {
    scriptTest(Clickstream.INSTANCE.getScripts().get(0), true, false);
  }

}
