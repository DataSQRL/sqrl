/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.data.Nutshop;
import com.datasqrl.util.data.Retail;
import com.datasqrl.util.data.Sensors;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class TestCmd {

  private static final Path OUTPUT_DIR = Paths.get("src", "test", "resources", "output");
  private static final Path SUBSCRIPTION_PATH = Paths.get("src/test/resources/subscriptions");
  private static final Path CC_PATH = Paths.get("src/test/resources/creditcard");

  protected Path buildDir = null;
  SnapshotTest.Snapshot snapshot;

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    clearDir(OUTPUT_DIR);
    Files.createDirectories(OUTPUT_DIR);
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
  }

  @AfterEach
  public void clearDirs() throws IOException {
    clearDir(OUTPUT_DIR);
    clearDir(buildDir);
  }

  protected void createSnapshot() {
    snapshot.addContent(FileTestUtil.getAllFilesAsString(OUTPUT_DIR), "output");
    if (buildDir != null) {
      snapshot.addContent(FileTestUtil.getAllFilesAsString(buildDir), "build");
    }
    snapshot.createOrValidate();
  }

  protected void clearDir(Path dir) throws IOException {
    if (dir != null && Files.isDirectory(dir)) {
      FileUtils.deleteDirectory(dir.toFile());
    }
  }

  @Test
  public void compileMutations() {
    Path root = Paths.get("../../sqrl-examples/mutations");
    execute(root,
        "compile", root.resolve("script.sqrl").toString(),
        root.resolve("schema.graphqls").toString());
  }

  @Test
  public void compileSubscriptions() {
    execute(SUBSCRIPTION_PATH,
        "compile", SUBSCRIPTION_PATH.resolve("script.sqrl").toString(),
        SUBSCRIPTION_PATH.resolve("schema.graphqls").toString());
  }

  @Test
  public void validateTest() {
    execute(SUBSCRIPTION_PATH,
        "validate", SUBSCRIPTION_PATH.resolve("script.sqrl").toString(),
        SUBSCRIPTION_PATH.resolve("schema.graphqls").toString());
  }

  @Test
  public void compileSubscriptionsInvalidGraphql() {
    execute(SUBSCRIPTION_PATH, ERROR_STATUS_HOOK,
        "compile", SUBSCRIPTION_PATH.resolve("invalidscript.sqrl").toString(),
        SUBSCRIPTION_PATH.resolve("invalidschema.graphqls").toString());
    snapshot.createOrValidate();
  }

  @Test
  public void discoverNutshop() {
    execute(Nutshop.INSTANCE.getRootPackageDirectory(),
        "discover", Nutshop.INSTANCE.getDataDirectory().toString(), "-o", OUTPUT_DIR.toString(), "-l", "3600");
    createSnapshot();
  }

  @Test
  @Disabled
  public void discoverExternal() {
    execute(Sensors.INSTANCE.getRootPackageDirectory(),
        "discover", Sensors.INSTANCE.getRootPackageDirectory().resolve("patientdata").toString(),
        "-o", Sensors.INSTANCE.getRootPackageDirectory().resolve("patientpackage").toString(), "-l", "3600");
  }

  @Test
  public void compileNutshop() {
    Path rootDir = Nutshop.INSTANCE.getRootPackageDirectory();
    buildDir = rootDir.resolve("build");

    TestScript script = Nutshop.INSTANCE.getScripts().get(1);
    execute(rootDir, "compile",
        script.getScriptPath().toString(),
        script.getGraphQLSchemas().get(0).getSchemaPath().toString(),
        "-t", OUTPUT_DIR.toString(),
        "--nolookup",
        "--mnt", rootDir.toString());
    createSnapshot();
  }

  @Test
  @SneakyThrows
  public void compileError() {
    Path rootDir = Nutshop.INSTANCE.getRootPackageDirectory();
    buildDir = rootDir.resolve("build");

    TestScript script = Nutshop.INSTANCE.getScripts().get(1);

    int statusCode = execute(rootDir, StatusHook.NONE,"compile",
        script.getScriptPath().toString(),
        "doesNotExist.graphql");
    assertEquals(1, statusCode, "Non-zero status code expected");
  }

  @Test
  @SneakyThrows
  public void compileNutshopWithSchema() {
    Path rootDir = Nutshop.INSTANCE.getRootPackageDirectory();
    buildDir = rootDir.resolve("build");

    TestScript script = Nutshop.INSTANCE.getScripts().get(1);
    execute(rootDir, "compile",
            script.getScriptPath().toString(),
            script.getGraphQLSchemas().get(0).getSchemaPath().toString(),
            "-t", OUTPUT_DIR.toString(), "-a", "GraphQL");
    Path schemaFile = rootDir.resolve(ScriptConfiguration.GRAPHQL_NORMALIZED_FILE_NAME);
    assertTrue(Files.isRegularFile(schemaFile),
        () -> String.format("Schema file could not be found: %s", schemaFile));
    Files.deleteIfExists(schemaFile);
    createSnapshot();
  }

  @Test
  public void compileRetail() {
    Path rootDir = Retail.INSTANCE.getRootPackageDirectory();
    buildDir = rootDir.resolve("build");

    TestScript script = Retail.INSTANCE.getScript(Retail.RetailScriptNames.FULL);
    execute(rootDir, "compile",
        script.getScriptPath().toString(),
        "-t", OUTPUT_DIR.toString(),
        "--nolookup");
    createSnapshot();
  }

  @Test
  public void discoverRetail() {
    execute(Retail.INSTANCE.getRootPackageDirectory(),
        "discover", Retail.INSTANCE.getDataDirectory().toString(), "-o", OUTPUT_DIR.toString());
    createSnapshot();
  }

  // SQRL #479 - Infinite loop replication
  @Test
  @Disabled
  public void testCreditCardInfiniteLoop() {
    Path basePath = CC_PATH;
    execute(basePath,
        "compile", basePath.resolve("creditcard.sqrl").toString(), basePath.resolve("creditcard.graphqls").toString());
  }

  public static int execute(Path rootDir, String... args) {
    return execute(rootDir, AssertStatusHook.INSTANCE, args);
  }
  public static int execute(Path rootDir, StatusHook hook, String... args) {
    return new RootCommand(rootDir,hook).getCmd().execute(args);
  }

  public class ErrorStatusHook implements StatusHook {
    @Override
    public void onSuccess() {

    }

    @Override
    public void onFailure(Exception e, ErrorCollector errors) {
      snapshot.addContent(ErrorPrinter.prettyPrint(errors));
    }
  }

  public final StatusHook ERROR_STATUS_HOOK = new ErrorStatusHook();
}
