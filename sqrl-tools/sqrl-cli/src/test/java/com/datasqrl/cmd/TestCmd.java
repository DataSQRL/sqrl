/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.packager.config.ScriptConfiguration;
import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.data.Nutshop;
import com.datasqrl.util.data.Retail;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class TestCmd {

  public static final Path OUTPUT_DIR = Paths.get("src", "test", "resources", "output");

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
    Path root = Paths.get("src/test/resources/subscriptions");
    execute(root,
        "compile", root.resolve("script.sqrl").toString(),
        root.resolve("schema.graphqls").toString());
  }

  @Test
  public void discoverNutshop() {
    execute(Nutshop.INSTANCE.getRootPackageDirectory(),
        "discover", Nutshop.INSTANCE.getDataDirectory().toString(), "-o", OUTPUT_DIR.toString(), "-l", "3600");
    createSnapshot();
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
        "--nolookup");
    createSnapshot();
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

  public static void execute(Path rootDir, String... args) {
    new RootCommand(rootDir, AssertStatusHook.INSTANCE, List.of(FeatureFlag.SUBSCRIPTIONS)).getCmd().execute(args);
  }
}
