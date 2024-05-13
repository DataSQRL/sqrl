package com.datasqrl.discovery;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.AbstractAssetSnapshotTest;
import com.datasqrl.actions.WriteDag;
import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.discovery.FlexibleSchemaInferencePreprocessorTest.DataFiles;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.preprocess.CopyStaticDataPreprocessor;
import com.datasqrl.packager.preprocess.Preprocessor.ProcessorContext;
import com.datasqrl.packager.util.FileHash;
import com.datasqrl.util.SnapshotTest.Snapshot;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class CopyStaticDataPreprocessorTest extends AbstractAssetSnapshotTest {

  public static final Path FILES_DIR = getResourcesDirectory("discoveryfiles");

  CopyStaticDataPreprocessor preprocessor = new CopyStaticDataPreprocessor();

  protected CopyStaticDataPreprocessorTest() {
    super(FILES_DIR.resolve("output"), AssertStatusHook.INSTANCE);
    super.buildDir = super.deployDir;
  }

  @ParameterizedTest
  @ArgumentsSource(DataFiles.class)
  @SneakyThrows
  void testScripts(Path file) {
    assertTrue(Files.exists(file));
    String filename = file.getFileName().toString();
    assertTrue(preprocessor.getPattern().matcher(filename).matches());
    this.snapshot = Snapshot.of(getDisplayName(file), getClass());
    preprocessor.processFile(file, new ProcessorContext(deployDir, buildDir, null), ErrorCollector.root());
    Path copyFile = deployDir.resolve(WriteDag.DATA_DIR).resolve(filename);
    assertTrue(Files.exists(copyFile));
    assertTrue(Files.isRegularFile(copyFile));
    snapshot.addContent(FileHash.getFor(copyFile), filename);
    snapshot.createOrValidate();
  }

}
