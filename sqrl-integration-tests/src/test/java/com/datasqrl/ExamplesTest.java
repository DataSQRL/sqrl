package com.datasqrl;

import static org.junit.jupiter.api.Assertions.assertFalse;

import com.datasqrl.IntegrationTestSettings.DatabaseEngine;
import com.datasqrl.IntegrationTestSettings.StreamEngine;
import com.datasqrl.discovery.DataDiscovery;
import com.datasqrl.discovery.TableWriter;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.impl.file.DirectoryDataSystemConfig;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.PackagerConfig;
import com.datasqrl.packager.config.Dependency;
import com.datasqrl.packager.repository.Repository;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.data.Examples;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class ExamplesTest extends AbstractPhysicalSQRLIT {

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {

    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
  }

//  @Disabled
  @SneakyThrows
  @ParameterizedTest
  @ArgumentsSource(TestScript.ExampleScriptsProvider.class)
  public void test(TestScript script) {
    Packager packager = PackagerConfig.builder()
        .rootDir(script.getRootPackageDirectory())
        .mainScript(script.getScriptPath())
        .graphQLSchemaFile(script.getGraphQLSchemas().isEmpty() ? null : script.getGraphQLSchemas().get(0).getSchemaPath())
        .repository(new Repository() {
          @Override
          public boolean retrieveDependency(Path targetPath, Dependency dependency)
              throws IOException {
            return false;
          }

          @Override
          public Optional<Dependency> resolveDependency(String packageName) {
            return Optional.empty();
          }
        }).build()
        .getPackager(ErrorCollector.root());
    Path build = packager.populateBuildDir(false);

    initialize(IntegrationTestSettings.builder()
        .database(DatabaseEngine.POSTGRES)
        .stream(StreamEngine.FLINK)
        .debugger(DebuggerConfig.NONE)
        .build(), build.getParent());

    discover(script);

    try {
      validateTables(script.getScript(), script.getResultTables()
        .toArray(new String[0]));
    } catch (Exception e) {
      System.err.println(ErrorPrinter.prettyPrint(errors));
      throw e;
    }
  }

  @Disabled
  @Test
  public void testSingle() {
    TestScript script = Examples.scriptList.get(Examples.scriptList.size()-1);
    System.out.println("Running Example: " + script.getName());
    test(script);
  }


  private void discover(TestScript script) {
    for (Path dataDir : script.getDataDirs()) {
      discover(script, dataDir);
    }
  }
  private void discover(TestScript script, Path dataDir) {
    ErrorCollector errors = ErrorCollector.root();
    DataDiscovery discovery = new DataDiscovery(errors, engineSettings);
    DataSystemConfig.DataSystemConfigBuilder builder = DataSystemConfig.builder();
    builder.datadiscovery(DirectoryDataSystemConfig.ofDirectory(dataDir));
    builder.type(ExternalDataType.source);
    builder.name(dataDir.getFileName().toString());

    DataSystemConfig systemConfig = builder.build();
    List<TableSource> sourceTables = discovery.runFullDiscovery(systemConfig);
    assertFalse(errors.isFatal(), errors.toString());

    write(script, dataDir, sourceTables);
  }

  @SneakyThrows
  private void write(TestScript script, Path dataDir, List<TableSource> sources) {
    //write files
    TableWriter writer = new TableWriter();
    Path path = script.getRootPackageDirectory()
        .resolve(dataDir.getFileName());
    path.toFile().mkdirs();
    writer.writeToFile(path, sources);
  }
}
