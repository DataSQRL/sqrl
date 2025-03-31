
/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import static com.datasqrl.config.ScriptConfigImpl.GRAPHQL_NORMALIZED_FILE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import java.nio.file.Paths;
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
public class TestCmd {
  public static final String PLAN_SQL = "flink-plan.sql";

  private static final Path OUTPUT_DIR = Paths.get("src", "test", "resources", "output");

  private static final Path RESOURCES = Paths.get("src/test/resources/");
  private static final Path SUBSCRIPTION_PATH = RESOURCES.resolve("subscriptions");
  private static final Path CC_PATH = RESOURCES.resolve("creditcard");
  private static final Path PROFILES_PATH = RESOURCES.resolve("profiles");
  private static final Path AVRO_PATH = RESOURCES.resolve("avro");
  private static final Path JSON_PATH = RESOURCES.resolve("json");

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
        "compile", "script.sqrl",
        "schema.graphqls");
  }

  @Test
  public void compileSubscriptions() {
    execute(SUBSCRIPTION_PATH,
        "compile", "script.sqrl",
       "schema.graphqls");
  }

  @Test
  public void validateTest() {
    execute(SUBSCRIPTION_PATH,
        "validate", "script.sqrl",
       "schema.graphqls");
  }

  @Test
  public void compileSubscriptionsInvalidGraphql() {
    execute(SUBSCRIPTION_PATH, ERROR_STATUS_HOOK,
        "compile","invalidscript.sqrl",
       "invalidschema.graphqls");
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
        "discover", "patientdata",
        "-o", "patientpackage", "-l", "3600");
  }

  @Test
  public void compileNutshop() {
    Path rootDir = Nutshop.INSTANCE.getRootPackageDirectory();
    buildDir = rootDir.resolve("build");

    TestScript script = Nutshop.INSTANCE.getScripts().get(1);
    execute(rootDir, "compile",
        script.getRootPackageDirectory().relativize(script.getScriptPath()).toString(),
        script.getRootPackageDirectory().relativize(script.getGraphQLSchemas().get(0).getSchemaPath()).toString(),
        "-t", OUTPUT_DIR.toString(),
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
        script.getRootPackageDirectory().relativize(script.getScriptPath()).toString(),
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
        script.getRootPackageDirectory().relativize(script.getScriptPath()).toString(),
        script.getRootPackageDirectory().relativize(script.getGraphQLSchemas().get(0).getSchemaPath()).toString(),
            "-t", OUTPUT_DIR.toString(), "-a", "GraphQL");
    Path schemaFile = rootDir.resolve(GRAPHQL_NORMALIZED_FILE_NAME);
    assertTrue(Files.isRegularFile(schemaFile),
        () -> String.format("Schema file could not be found: %s", schemaFile));
    Files.deleteIfExists(schemaFile);
    createSnapshot();
  }

  @Test
  public void compileJsonDowncasting() {
    // Should have a raw json type for postgres and a jsontostring for kafka
    compilePlan(JSON_PATH, null, null);
  }

  @Test
  public void compileFlexibleJson() {
    // Should have a raw json type for postgres and a jsontostring for kafka
    compilePlan(RESOURCES.resolve("flexible-json"), "package.json", null);
  }

  @Test
  public void compileAvroWOdatabase() {
    compilePlan(AVRO_PATH, "packageWOdatabase.json", "schema.graphqls");
  }

  @Test
  public void compileAvroWOserver() {
    compilePlan(AVRO_PATH, "packageWOserver.json", "schema.graphqls");
  }

  @Test
  public void compileAvroWserverdatabase() {
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

    execute(path, args.toArray(a->new String[a]));
    snapshotSql();
  }

  @SneakyThrows
  private void snapshotSql() {
    snapshot.addContent(Files.readString(OUTPUT_DIR.resolve(PLAN_SQL)),
        "sql");
    snapshot.createOrValidate();
  }

  @Test
  public void compileRetail() {
    Path rootDir = Retail.INSTANCE.getRootPackageDirectory();
    buildDir = rootDir.resolve("build");

    TestScript script = Retail.INSTANCE.getScript(Retail.RetailScriptNames.FULL);
    execute(rootDir, "compile",
        script.getRootPackageDirectory().relativize(script.getScriptPath()).toString(),
        "-t", OUTPUT_DIR.toString());
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
  public void testCreditCardInfiniteLoop() {
    Path basePath = CC_PATH;
    execute(basePath,
        "compile", "creditcard.sqrl",
        "creditcard.graphqls");
  }

  public static int execute(Path rootDir, String... args) {
    return execute(rootDir, new AssertStatusHook(), args);
  }

  public static int execute(Path rootDir, StatusHook hook, String... args) {
    RootCommand rootCommand = new RootCommand(rootDir, hook);
    int exitCode = rootCommand.getCmd().execute(args) + (hook.isSuccess() ? 0 : 1);
    if (exitCode != 0) {
      fail();
    }
    return exitCode;
  }

  public class ErrorStatusHook implements StatusHook {

    private boolean failed;

    @Override
    public void onSuccess(ErrorCollector errors) {

    }

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
