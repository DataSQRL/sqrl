package com.datasqrl.discovery;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.AbstractAssetSnapshotTest;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.discovery.preprocessor.FlexibleSchemaInferencePreprocessor;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.inject.SqrlInjector;
import com.datasqrl.inject.StatefulModule;
import com.datasqrl.packager.preprocess.Preprocessor.ProcessorContext;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.util.SnapshotTest.Snapshot;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.SqrlSchema;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class FlexibleSchemaInferencePreprocessorTest extends AbstractAssetSnapshotTest {

  public static final Path FILES_DIR = getResourcesDirectory("discoveryfiles");

  ErrorCollector errors = ErrorCollector.root();
  FlexibleSchemaInferencePreprocessor preprocessor;
  PackageJson packageJson;

  protected FlexibleSchemaInferencePreprocessorTest() {
    super(FILES_DIR.resolve("output"));
    packageJson = SqrlConfigCommons.fromFilesPackageJson(errors, List.of(Path.of("../../profiles/default/package.json")));
    Injector injector = Guice.createInjector(
        new SqrlInjector(ErrorCollector.root(), FILES_DIR, super.deployDir, packageJson, ExecutionGoal.COMPILE, null),
        new StatefulModule(new SqrlSchema(new TypeFactory(), NameCanonicalizer.SYSTEM)));
    preprocessor = injector.getInstance(FlexibleSchemaInferencePreprocessor.class);
    super.buildDir = deployDir;
  }


  @ParameterizedTest
  @ArgumentsSource(DataFiles.class)
  @SneakyThrows
  void testScripts(Path file) {
    assertTrue(Files.exists(file));
    Path targetFile = Files.copy(file, deployDir.resolve(file.getFileName()));
    String filename = file.getFileName().toString();
    assertTrue(preprocessor.getPattern().matcher(filename).matches());
    this.snapshot = Snapshot.of(getDisplayName(file), getClass());
    preprocessor.processFile(targetFile, new ProcessorContext(deployDir, buildDir, packageJson), errors);
    createSnapshot();
  }


  @Override
  public Predicate<Path> getDeployDirFilter() {
    return p -> p.getFileName().toString().endsWith("table.json")
        || p.getFileName().toString().endsWith("schema.yml");
  }

  static class DataFiles implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {
      return Files.list(FILES_DIR).map(Arguments::of);
    }
  }

}
