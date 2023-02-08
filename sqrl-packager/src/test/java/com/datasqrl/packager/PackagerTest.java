/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.config.ConfigurationTest;
import com.datasqrl.packager.config.Dependency;
import com.datasqrl.packager.repository.Repository;
import com.datasqrl.spi.ManifestConfiguration;
import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.data.Retail;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class PackagerTest {

  SnapshotTest.Snapshot snapshot;

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
  }

  @Test
  public void testRetailPackaging() {
    TestScript script = Retail.INSTANCE.getScript(Retail.RetailScriptNames.FULL);
    Path graphQLSchema = script.getRootPackageDirectory().resolve("c360-full-graphqlv1")
        .resolve("schema.graphqls");
    Path packageFileWithoutManifest = script.getRootPackageDirectory()
        .resolve("package-exampleWOmanifest.json");
    Path packageFileWithManifest = script.getRootPackageDirectory()
        .resolve("package-exampleWmanifest.json");

    testCombination(script.getScriptPath(), null, null);
    testCombination(script.getScriptPath(), null, packageFileWithoutManifest);
    testCombination(script.getScriptPath(), null, packageFileWithManifest);
    testCombination(null, null, packageFileWithManifest);
    testCombination(script.getScriptPath(), graphQLSchema, packageFileWithoutManifest);
    testCombination(script.getScriptPath(), graphQLSchema, packageFileWithManifest);
    testCombination(null, graphQLSchema, packageFileWithManifest);

    snapshot.createOrValidate();
  }

  private static final Path baseDependencyPath = ConfigurationTest.RESOURCE_DIR.resolve("dependency");
  private static final List<Path> depScripts = new ArrayList<>();
  static {
    IntStream.rangeClosed(1,2)
        .forEach(i -> depScripts.add(baseDependencyPath.resolve("main" + i + ".sqrl")));
  }

  @Test
  public void dependencyResolution() {
    Path pkgWDeps = baseDependencyPath.resolve("packageWDependencies.json");
    Path pkgWODeps = baseDependencyPath.resolve("packageWODependencies.json");

    testCombinationMockRepo(depScripts.get(0),pkgWDeps);
    testCombinationMockRepo(depScripts.get(0),pkgWODeps);
    testCombinationMockRepo(depScripts.get(1),pkgWDeps);
    testCombinationMockRepo(depScripts.get(1),pkgWODeps);

    snapshot.createOrValidate();
  }

  @Test
  @Disabled //Requires running repository
  public void testRemoteRepository() {
    testCombination(depScripts.get(0), null, null, null);
    snapshot.createOrValidate();
  }

  private void testCombinationMockRepo(Path main, Path packageFile) {
    testCombination(main,null,packageFile,new MockRepository());
  }

  private void testCombination(Path main, Path graphQl, Path packageFile) {
    testCombination(main,graphQl,packageFile,new MockRepository());
  }

  @SneakyThrows
  private void testCombination(Path main, Path graphQl, Path packageFile, Repository repository) {
    PackagerConfig config = buildPackagerConfig(main, graphQl, packageFile, repository);
    Packager pkg = config.getPackager(ErrorCollector.root());
    pkg.cleanUp();
    populateBuildDirAndTakeSnapshot(pkg, main, graphQl, packageFile);
    pkg.cleanUp();
  }

  private PackagerConfig buildPackagerConfig(Path main, Path graphQl, Path packageFile,
      Repository repository) {
    PackagerConfig.PackagerConfigBuilder builder = PackagerConfig.builder();
    if (main != null) {
      builder.mainScript(main);
    }
    if (graphQl != null) {
      builder.graphQLSchemaFile(graphQl);
    }
    if (packageFile != null) {
      builder.packageFiles(List.of(packageFile));
    }
    if (repository != null) {
      builder.repository(repository);
    }
    return builder.build();
  }

  @SneakyThrows
  private void populateBuildDirAndTakeSnapshot(Packager pkg, Path main, Path graphQl, Path packageFile) {
    Path buildDir = pkg.getRootDir().resolve(Packager.BUILD_DIR_NAME);
    pkg.populateBuildDir(true);
    String[] caseNames = Stream.of(main, graphQl, packageFile)
        .filter(Predicate.not(Objects::isNull)).map(String::valueOf)
        .toArray(size -> new String[size + 1]);
    caseNames[caseNames.length - 1] = "dir";
    snapshot.addContent(FileTestUtil.getAllFilesAsString(buildDir), caseNames);
    caseNames[caseNames.length - 1] = "package";
    snapshot.addContent(Files.readString(buildDir.resolve(Packager.PACKAGE_FILE_NAME)), caseNames);
  }

  private static class MockRepository implements Repository {

    @Override
    public boolean retrieveDependency(Path targetPath, Dependency dependency) throws IOException {
      Files.createDirectories(targetPath);
      Files.writeString(targetPath.resolve("package.json"),"test");
      return true;
    }

    @Override
    public Optional<Dependency> resolveDependency(String packageName) {
      if (NUTSHOP.getName().equals(packageName)) {
        return Optional.of(NUTSHOP);
      }
      return Optional.empty();
    }

    private static final Dependency NUTSHOP = new Dependency("datasqrl.examples.Nutshop","0.1.0","dev");

  }

}
