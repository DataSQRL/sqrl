/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.data.Nutshop;
import com.datasqrl.util.data.Retail;
import com.datasqrl.util.data.Sensors;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Disabled
public class TestCommand {
  public static final String PLAN_SQL = "flink-plan.sql";

  private static final Path OUTPUT_DIR = Path.of("src", "test", "resources", "output");

  private static final Path RESOURCES = Path.of("src/test/resources/");
  private static final Path SUBSCRIPTION_PATH = RESOURCES.resolve("subscriptions");
  private static final Path CC_PATH = RESOURCES.resolve("creditcard");
  private static final Path AVRO_PATH = RESOURCES.resolve("avro");
  private static final Path JSON_PATH = RESOURCES.resolve("json");

  protected Path buildDir = null;
  SnapshotTest.Snapshot snapshot;

  @BeforeEach
  void setup(TestInfo testInfo) throws IOException {
    clearDir(OUTPUT_DIR);
    Files.createDirectories(OUTPUT_DIR);
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
  }

  @AfterEach
  void clearDirs() throws IOException {
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
  void compileMutations() {
    var root = Path.of("../../sqrl-examples/mutations");
    execute(root, "compile", "script.sqrl", "schema.graphqls");
  }

  @Test
  void compileSubscriptions() {
    execute(SUBSCRIPTION_PATH, "compile", "script.sqrl", "schema.graphqls");
  }

  @Test
  void validateTest() {
    execute(SUBSCRIPTION_PATH, "validate", "script.sqrl", "schema.graphqls");
  }

  @Test
  void compileSubscriptionsInvalidGraphql() {
    execute(
        SUBSCRIPTION_PATH,
        ERROR_STATUS_HOOK,
        "compile",
        "invalidscript.sqrl",
        "invalidschema.graphqls");
    snapshot.createOrValidate();
  }

  @Test
  void discoverNutshop() {
    execute(
        Nutshop.INSTANCE.getRootPackageDirectory(),
        "discover",
        Nutshop.INSTANCE.getDataDirectory().toString(),
        "-o",
        OUTPUT_DIR.toString(),
        "-l",
        "3600");
    createSnapshot();
  }

  @Test
  @Disabled
  void discoverExternal() {
    execute(
        Sensors.INSTANCE.getRootPackageDirectory(),
        "discover",
        "patientdata",
        "-o",
        "patientpackage",
        "-l",
        "3600");
  }

  @Test
  void compileNutshop() {
    var rootDir = Nutshop.INSTANCE.getRootPackageDirectory();
    buildDir = rootDir.resolve("build");

    var script = Nutshop.INSTANCE.getScripts().get(1);
    execute(
        rootDir,
        "compile",
        script.getRootPackageDirectory().relativize(script.getScriptPath()).toString(),
        script
            .getRootPackageDirectory()
            .relativize(script.getGraphQLSchemas().get(0).getSchemaPath())
            .toString(),
        "-t",
        OUTPUT_DIR.toString(),
        "--mnt",
        rootDir.toString());
    createSnapshot();
  }

  @Test
  @SneakyThrows
  void compileError() {
    Path rootDir = Nutshop.INSTANCE.getRootPackageDirectory();
    buildDir = rootDir.resolve("build");

    TestScript script = Nutshop.INSTANCE.getScripts().get(1);

    int statusCode =
        execute(
            rootDir,
            StatusHook.NONE,
            "compile",
            script.getRootPackageDirectory().relativize(script.getScriptPath()).toString(),
            "doesNotExist.graphql");
    assertThat(statusCode).as("Non-zero status code expected").isEqualTo(1);
  }

  @Test
  @SneakyThrows
  void compileNutshopWithSchema() {
    Path rootDir = Nutshop.INSTANCE.getRootPackageDirectory();
    buildDir = rootDir.resolve("build");

    TestScript script = Nutshop.INSTANCE.getScripts().get(1);
    execute(
        rootDir,
        "compile",
        script.getRootPackageDirectory().relativize(script.getScriptPath()).toString(),
        script
            .getRootPackageDirectory()
            .relativize(script.getGraphQLSchemas().get(0).getSchemaPath())
            .toString(),
        "-t",
        OUTPUT_DIR.toString(),
        "-a",
        "GraphQL");
    Path schemaFile = rootDir.resolve("schema.graphqls");
    assertThat(Files.isRegularFile(schemaFile))
        .as(() -> "Schema file could not be found: %s".formatted(schemaFile))
        .isTrue();
    Files.deleteIfExists(schemaFile);
    createSnapshot();
  }

  @Test
  void compileJsonDowncasting() {
    // Should have a raw json type for postgres and a jsontostring for kafka
    compilePlan(JSON_PATH, null, null);
  }

  @Test
  void compileFlexibleJson() {
    // Should have a raw json type for postgres and a jsontostring for kafka
    compilePlan(RESOURCES.resolve("flexible-json"), "package.json", null);
  }

  @Test
  void compileAvroWOdatabase() {
    compilePlan(AVRO_PATH, "packageWOdatabase.json", "schema.graphqls");
  }

  @Test
  void compileAvroWOserver() {
    compilePlan(AVRO_PATH, "packageWOserver.json", "schema.graphqls");
  }

  @Test
  void compileAvroWserverdatabase() {
    compilePlan(AVRO_PATH, "packageWserverdatabase.json", "schema.graphqls");
  }

  private void compilePlan(Path path, String pkg, String graphql) {
    buildDir = path.resolve("build");
    List<String> args = new ArrayList<>();
    args.add("compile");
    args.add("c360.sqrl");
    args.add("-t");
    args.add(OUTPUT_DIR.toString());

    if (pkg != null) {
      args.add("-c");
      args.add(path.resolve(pkg).toString());
    }
    if (graphql != null) {
      args.add(graphql);
    }

    execute(path, args.toArray(String[]::new));
    snapshotSql();
  }

  @SneakyThrows
  private void snapshotSql() {
    snapshot.addContent(Files.readString(OUTPUT_DIR.resolve(PLAN_SQL)), "sql");
    snapshot.createOrValidate();
  }

  @Test
  void compileRetail() {
    var rootDir = Retail.INSTANCE.getRootPackageDirectory();
    buildDir = rootDir.resolve("build");

    var script = Retail.INSTANCE.getScript(Retail.RetailScriptNames.FULL);
    execute(
        rootDir,
        "compile",
        script.getRootPackageDirectory().relativize(script.getScriptPath()).toString(),
        "-t",
        OUTPUT_DIR.toString());
    createSnapshot();
  }

  @Test
  void discoverRetail() {
    execute(
        Retail.INSTANCE.getRootPackageDirectory(),
        "discover",
        Retail.INSTANCE.getDataDirectory().toString(),
        "-o",
        OUTPUT_DIR.toString());
    createSnapshot();
  }

  // SQRL #479 - Infinite loop replication
  @Test
  void creditCardInfiniteLoop() {
    execute(CC_PATH, "compile", "creditcard.sqrl", "creditcard.graphqls");
  }

  public static int execute(Path rootDir, String... args) {
    return execute(rootDir, new AssertStatusHook(), args);
  }

  public static int execute(Path rootDir, StatusHook hook, String... args) {
    var cli = new DatasqrlCli(rootDir, hook, true);
    var exitCode = cli.getCmd().execute(args) + (hook.isSuccess() ? 0 : 1);
    if (exitCode != 0) {
      fail("");
    }
    return exitCode;
  }

  public class ErrorStatusHook implements StatusHook {

    private boolean failed;

    @Override
    public void onSuccess(ErrorCollector errors) {}

    @Override
    public void onFailure(Throwable e, ErrorCollector errors) {
      snapshot.addContent(ErrorPrinter.prettyPrint(errors));
    }

    @Override
    public boolean isSuccess() {
      return !failed;
    }

    @Override
    public boolean isFailed() {
      return failed;
    }
  }

  public final StatusHook ERROR_STATUS_HOOK = new ErrorStatusHook();
}
