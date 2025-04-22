package com.datasqrl.discovery;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import com.datasqrl.AbstractAssetSnapshotTest;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.discovery.FlexibleSchemaInferencePreprocessorTest.DataFiles;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.preprocess.CopyStaticDataPreprocessor;
import com.datasqrl.packager.preprocess.Preprocessor.ProcessorContext;
import com.datasqrl.packager.util.FileHash;
import com.datasqrl.util.SnapshotTest.Snapshot;

import lombok.SneakyThrows;

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
    assertThat(file).exists();
    String filename = file.getFileName().toString();
    assertThat(filename).matches(preprocessor.getPattern());
    this.snapshot = Snapshot.of(getDisplayName(file), getClass());
    preprocessor.processFile(file, new ProcessorContext(outputDir, buildDir, null), ErrorCollector.root());
    Path copyFile = outputDir.resolve(SqrlConstants.DATA_DIR).resolve(filename);
    assertThat(copyFile).exists().isRegularFile();
    snapshot.addContent(FileHash.getFor(copyFile), filename);
    snapshot.createOrValidate();
  }

}
