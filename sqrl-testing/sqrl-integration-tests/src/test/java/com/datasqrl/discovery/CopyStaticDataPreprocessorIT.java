package com.datasqrl.discovery;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.AbstractAssetSnapshotTest;
import com.datasqrl.config.SqrlConstants;
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

public class CopyStaticDataPreprocessorIT extends AbstractAssetSnapshotTest {

  public static final Path FILES_DIR = getResourcesDirectory("discoveryfiles");

  CopyStaticDataPreprocessor preprocessor = new CopyStaticDataPreprocessor();

  protected CopyStaticDataPreprocessorIT() {
    super(FILES_DIR.resolve("output"));
    super.buildDir = super.outputDir;
  }

  @ParameterizedTest
  @ArgumentsSource(DataFiles.class)
  @SneakyThrows
  void testScripts(Path file) {
    assertTrue(Files.exists(file));
    String filename = file.getFileName().toString();
    assertTrue(preprocessor.getPattern().matcher(filename).matches());
    this.snapshot = Snapshot.of(getDisplayName(file), getClass());
    preprocessor.processFile(file, new ProcessorContext(outputDir, buildDir, null), ErrorCollector.root());
    Path copyFile = outputDir.resolve(SqrlConstants.DATA_DIR).resolve(filename);
    assertTrue(Files.exists(copyFile));
    assertTrue(Files.isRegularFile(copyFile));
    snapshot.addContent(FileHash.getFor(copyFile), filename);
    snapshot.createOrValidate();
  }

}
