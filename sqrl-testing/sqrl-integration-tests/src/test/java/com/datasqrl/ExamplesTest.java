package com.datasqrl;

import com.datasqrl.compile.Compiler.CompilerResult;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.discovery.TableWriter;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.module.resolver.FileResourceResolver;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.packager.Packager;
import com.datasqrl.packager.PackagerConfig;
import com.datasqrl.packager.config.Dependency;
import com.datasqrl.packager.repository.Repository;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.data.Examples;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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
    errors = ErrorCollector.root();
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
  }

  @Disabled
  @SneakyThrows
  @ParameterizedTest
  @ArgumentsSource(TestScript.ExampleScriptsProvider.class)
  public void test(TestScript script) {
    Packager packager = PackagerConfig.builder()
        .rootDir(script.getRootPackageDirectory())
        .mainScript(script.getScriptPath())
        .config(SqrlConfigCommons.fromFiles(errors,script.getRootPackageDirectory().resolve("package.json")))
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

//    initialize(IntegrationTestSettings.builder()
//        .database(DatabaseEngine.POSTGRES)
//        .stream(StreamEngine.FLINK)
//        .debugger(DebuggerConfig.NONE)
//        .build(), build.getParent());

    Path buildDir = script.getRootPackageDirectory().resolve("build");
    com.datasqrl.compile.Compiler compiler = new com.datasqrl.compile.Compiler();
    CompilerResult result = compiler.run(ErrorCollector.root(),
        buildDir,
        false, buildDir.resolve("deploy"));

    System.out.println(result);
//    Path path = build.getParent().resolve("deploy");
//    Files.createDirectories(path);

//    discover(script);

    //Run schema script on db
//    Optional<URI> schema = resourceResolver.resolveFile(NamePath.of("deploy", "dbschema.json"));
//    Deserializer deserializer = new Deserializer();
//    List<String> schemaDDL = deserializer.getJsonMapper()
//        .readValue(schema.get().toURL(), new TypeReference<List<String>>(){});
//
//    execute(schemaDDL);

//    FlinkMain flinkMain = new FlinkMain();
//    flinkMain.run(resourceResolver);

    try {
//      validateTables(script.getScript(), script.getResultTables()
//        .toArray(new String[0]));
    } catch (Exception e) {
      System.err.println(ErrorPrinter.prettyPrint(errors));
      throw e;
    }
  }
  public ExecutionResult execute(List<String> dmls) {
    try (Connection conn = DriverManager.getConnection(
        "jdbc:h2:file:./h2.db",
        null,
        null)) {
      for (String dml : dmls) {
        try (Statement stmt = conn.createStatement()) {
//          log.trace("Creating: " + dml);
          stmt.executeUpdate(dml);
        } catch (SQLException e) {
          throw new RuntimeException("Could not execute SQL query", e);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not connect to database", e);
    }
    return new ExecutionResult.Message(
        String.format("Executed %d DDL statements", dmls.size()));
  }
  @Disabled
  @Test
  public void testSingle() {
    TestScript script = Examples.scriptList.get(Examples.scriptList.size()-1);
    System.out.println("Running Example: " + script.getName());
    test(script);
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
