package com.datasqrl.cmd;

import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.data.Nutshop;
import com.datasqrl.util.data.Retail;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
  public void discoverNutshop() {
    execute(Nutshop.INSTANCE.getRootPackageDirectory(),
        "discover", Nutshop.INSTANCE.getDataDirectory().toString(), "-o", OUTPUT_DIR.toString());
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
        "-o", OUTPUT_DIR.toString());
    createSnapshot();
  }

  @Test
  public void compileRetail() {
    Path rootDir = Retail.INSTANCE.getRootPackageDirectory();
    buildDir = rootDir.resolve("build");

    TestScript script = Retail.INSTANCE.getScript(Retail.RetailScriptNames.FULL);
    execute(rootDir, "compile",
        script.getScriptPath().toString(),
        "-o", OUTPUT_DIR.toString());
    createSnapshot();
  }

  @Test
  public void discoverRetail() {
    execute(Retail.INSTANCE.getRootPackageDirectory(),
        "discover", Retail.INSTANCE.getDataDirectory().toString(), "-o", OUTPUT_DIR.toString());
    createSnapshot();
  }

  public static void execute(Path rootDir, String... args) {
    new CommandLine(new RootCommand(rootDir)).execute(args);
  }

}
