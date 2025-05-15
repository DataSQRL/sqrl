package com.datasqrl.discovery;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import com.datasqrl.AbstractAssetSnapshotTest;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.discovery.preprocessor.FlexibleSchemaInferencePreprocessor;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.inject.SqrlInjector;
import com.datasqrl.packager.preprocessor.Preprocessor.ProcessorContext;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.util.SnapshotTest.Snapshot;
import com.google.inject.Guice;

import lombok.SneakyThrows;

public class FlexibleSchemaInferencePreprocessorTest extends AbstractAssetSnapshotTest {

  public static final Path FILES_DIR = getResourcesDirectory("discoveryfiles");

  ErrorCollector errors = ErrorCollector.root();
  FlexibleSchemaInferencePreprocessor preprocessor;
  PackageJson packageJson;

  protected FlexibleSchemaInferencePreprocessorTest() {
    super(FILES_DIR.resolve("output"));
    try {
      packageJson = SqrlConfigCommons.getDefaultPackageJson(errors);
    } catch (Exception e) {
      System.out.println(ErrorPrinter.prettyPrint(errors));
      throw e;
    }
    var injector =
        Guice.createInjector(
            new SqrlInjector(
                ErrorCollector.root(),
                FILES_DIR,
                super.outputDir,
                packageJson,
                ExecutionGoal.COMPILE));
    preprocessor = injector.getInstance(FlexibleSchemaInferencePreprocessor.class);
    super.buildDir = outputDir;
  }

  @ParameterizedTest
  @ArgumentsSource(DataFiles.class)
  @SneakyThrows
  void testScripts(Path file) {
    assertTrue(Files.exists(file));
    Path targetFile = Files.copy(file, outputDir.resolve(file.getFileName()));
    String filename = file.getFileName().toString();
    assertTrue(preprocessor.getPattern().matcher(filename).matches());
    this.snapshot = Snapshot.of(getDisplayName(file), getClass());
    preprocessor.processFile(
        targetFile, new ProcessorContext(outputDir, buildDir, packageJson), errors);
    createSnapshot();
  }

  @Override
  public Predicate<Path> getOutputDirFilter() {
    return p -> p.getFileName().toString().endsWith("table.sql");
  }

  static class DataFiles implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {
      return Files.list(FILES_DIR).map(Arguments::of);
    }
  }
}
