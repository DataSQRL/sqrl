/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.packager.config.ConfigurationTest;
import com.datasqrl.packager.config.Dependency;
import com.datasqrl.packager.repository.Repository;
import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.data.Retail;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
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
  private static final Path[] depScripts = new Path[3];
  static {
    for (int i = 0; i < depScripts.length; i++) {
      depScripts[i] = baseDependencyPath.resolve("main"+(i+1)+".sqrl");
    }
  }

  @Test
  public void dependencyResolution() {

    Path pkgWDeps = baseDependencyPath.resolve("packageWDependencies.json");
    Path pkgWODeps = baseDependencyPath.resolve("packageWODependencies.json");
    Path pkgMappedDeps = baseDependencyPath.resolve("packageWMappedDependencies.json");

    testCombinationMockRepo(depScripts[0],pkgWDeps);
    testCombinationMockRepo(depScripts[0],pkgWODeps);
    testCombinationMockRepo(depScripts[1],pkgWDeps);
    testCombinationMockRepo(depScripts[1],pkgWODeps);
    testCombinationMockRepo(depScripts[2],pkgMappedDeps);

    snapshot.createOrValidate();
  }

  @Test
  @Disabled //Requires running repository
  public void testRemoteRepository() {
    testCombination(depScripts[0], null, null);
    snapshot.createOrValidate();
  }

  private void testCombinationMockRepo(Path main, Path packageFile) {
    testCombination(main,null,packageFile,new MockRepository());
  }

  private void testCombination(Path main, Path graphQl, Path packageFile) {
    testCombination(main,graphQl,packageFile,null);
  }

  @SneakyThrows
  private void testCombination(Path main, Path graphQl, Path packageFile, Repository repository) {
    Packager.Config.ConfigBuilder builder = Packager.Config.builder();
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
    Packager pkg = builder.build().getPackager();
    pkg.cleanUp();
    pkg.populateBuildDir(true);
    Path buildDir = pkg.getRootDir().resolve(Packager.BUILD_DIR_NAME);
    String[] caseNames = Stream.of(main, graphQl, packageFile)
        .filter(Predicate.not(Objects::isNull)).map(String::valueOf)
        .toArray(size -> new String[size + 1]);
    caseNames[caseNames.length - 1] = "dir";
    snapshot.addContent(FileTestUtil.getAllFilesAsString(buildDir), caseNames);
    caseNames[caseNames.length - 1] = "package";
    snapshot.addContent(Files.readString(buildDir.resolve(Packager.PACKAGE_FILE_NAME)), caseNames);
    pkg.cleanUp();
  }

  private static class MockRepository implements Repository {

    @Override
    public boolean retrieveDependency(Path targetPath, Dependency dependency) throws IOException {
      assertEquals(NUTSHOP, dependency);
      Files.createDirectories(targetPath);
      Files.writeString(targetPath.resolve("package.json"),"test");
      return true;
    }

    @Override
    public Optional<Dependency> resolveDependency(String packageName) {
      assertEquals(NUTSHOP.getName(),packageName);
      return Optional.of(NUTSHOP);
    }

    private static final Dependency NUTSHOP = new Dependency("datasqrl.examples.Nutshop","0.1.0","dev");

  }

}
