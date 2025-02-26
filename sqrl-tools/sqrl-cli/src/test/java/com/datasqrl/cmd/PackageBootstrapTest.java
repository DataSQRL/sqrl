/// *
// * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
// */
// package com.datasqrl.cmd;
//
// import static com.datasqrl.config.PackageJson.ScriptConfig.GRAPHQL_NORMALIZED_FILE_NAME;
// import static com.datasqrl.packager.Packager.*;
// import static com.datasqrl.util.TestResources.RESOURCE_DIR;
// import static org.junit.jupiter.api.Assertions.assertEquals;
// import static org.mockito.ArgumentMatchers.any;
// import static org.mockito.Mockito.*;
//
// import com.datasqrl.config.IDependency;
// import com.datasqrl.config.PackageJson;
// import com.datasqrl.error.ErrorCollector;
// import com.datasqrl.packager.Packager;
// import com.datasqrl.config.Dependency;
// import com.datasqrl.packager.repository.Repository;
// import com.datasqrl.util.FileTestUtil;
// import com.datasqrl.util.SnapshotTest;
// import com.datasqrl.util.TestScript;
// import com.datasqrl.util.data.Retail;
// import java.io.IOException;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.util.*;
// import java.util.function.Predicate;
// import java.util.stream.IntStream;
// import java.util.stream.Stream;
// import lombok.SneakyThrows;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.Disabled;
// import org.junit.jupiter.api.Test;
// import org.junit.jupiter.api.TestInfo;
//
// public class PackageBootstrapTest {
//
//  SnapshotTest.Snapshot snapshot;
//
//  @BeforeEach
//  public void setup(TestInfo testInfo) throws IOException {
//    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
//  }
//
//  // this test is supposed to test package json merging scenarios
//  @Disabled
//  @Test
//  public void testRetailPackaging() {
//    TestScript script = Retail.INSTANCE.getScript(Retail.RetailScriptNames.FULL);
//
//    Path graphQLSchema = script
//            .getRootPackageDirectory()
//            .resolve("c360-full-graphqlv1")
//            .resolve(GRAPHQL_NORMALIZED_FILE_NAME);
//
//    Path packageFileWithoutScript = script
//            .getRootPackageDirectory()
//            .resolve("package-exampleWOscript.json");
//
//    Path packageFileWithScript = script
//            .getRootPackageDirectory()
//            .resolve("package-exampleWscript.json");
//
//    testCombination(script.getScriptPath(), null, packageFileWithoutScript);
//    testCombination(script.getScriptPath(), null, packageFileWithScript);
//    testCombination(null, null, packageFileWithScript);
//    testCombination(script.getScriptPath(), graphQLSchema, packageFileWithoutScript);
//    testCombination(script.getScriptPath(), graphQLSchema, packageFileWithScript);
//    testCombination(null, graphQLSchema, packageFileWithScript);
//
//    snapshot.createOrValidate();
//  }
//
//  private static final Path baseDependencyPath = RESOURCE_DIR.resolve("dependency");
//  private static final List<Path> depScripts = new ArrayList<>();
//
//  static {
//    IntStream.rangeClosed(1, 2)
//        .forEach(i -> depScripts.add(baseDependencyPath.resolve("main" + i + ".sqrl")));
//  }
//
//  private static final Path pkgWDeps = baseDependencyPath.resolve("packageWDependencies.json");
//  private static final Path pkgWODeps = baseDependencyPath.resolve("packageWODependencies.json");
//
//  @Test
//  public void dependencyResolution() {
//    testCombinationMockRepo(depScripts.get(0), pkgWDeps);
//    testCombinationMockRepo(depScripts.get(0), pkgWODeps);
//    testCombinationMockRepo(depScripts.get(1), pkgWDeps);
//    testCombinationMockRepo(depScripts.get(1), pkgWODeps);
//    snapshot.createOrValidate();
//  }
//
//  @Test
//  public void testRemoteRepository() {
//    testCombination(depScripts.get(0), null, pkgWODeps);
//    testCombination(depScripts.get(0), null, pkgWDeps);
//    snapshot.createOrValidate();
//  }
//
//  // finish this once the immutable config list problem is solved
//  @Disabled
//  @Test
//  public void testProfileResolutionPreference() {
//    String[] profiles = {"test-profile"};
//
//    Repository mockRepo = mock(Repository.class);
//    testCombination(depScripts.get(0), null, null, profiles, mockRepo);
//
//    verify(mockRepo, never()).resolveDependency(any());
//  }
//
//  private void testCombinationMockRepo(Path main, Path packageFile) {
//    testCombination(main, null, packageFile, null, new MockRepository());
//  }
//
//  private void testCombination(Path main, Path graphQl, Path packageFile) {
//    testCombination(main, graphQl, packageFile, null, new MockRepository());
//  }
//
//  @SneakyThrows
//  private void testCombination(Path main, Path graphQl, Path packageFile, String[] profiles,
// Repository repository) {
//    ErrorCollector errors = ErrorCollector.root();
//    List<Path> files = new ArrayList<>();
//    if (main != null) {
//      files.add(packageFile.getParent().relativize(main));
//    }
//    if (graphQl != null) {
//      files.add(packageFile.getParent().relativize(graphQl));
//    }
//    if (profiles == null) {
//      profiles = new String[0];
//    }
//
//    PackageBootstrap bootstrap = new PackageBootstrap(
//            packageFile.getParent(),
//            List.of(packageFile.getFileName()),
//            profiles,
//            files.toArray(Path[]::new),
//            true);
//
//    PackageJson config = bootstrap.bootstrap(repository, errors, (e) -> null, (c) -> c, null);
//
//    Packager pkg = new Packager(repository, packageFile.getParent(), config, errors);
//
//    Path buildDir = packageFile.getParent().resolve(BUILD_DIR_NAME);
//
//    pkg.cleanBuildDir(buildDir);
//    populateBuildDirAndTakeSnapshot(pkg, main, graphQl, packageFile);
//    pkg.cleanBuildDir(buildDir);
//  }
//
//  @SneakyThrows
//  private void populateBuildDirAndTakeSnapshot(Packager pkg, Path main, Path graphQl, Path
// packageFile) {
//    Path buildDir = pkg.getRootDir().resolve(Packager.BUILD_DIR_NAME);
//    pkg.preprocess();
//    String[] caseNames =
//        Stream.of(main, graphQl, packageFile)
//            .filter(Predicate.not(Objects::isNull))
//            .map(String::valueOf)
//            .toArray(size -> new String[size + 1]);
//    caseNames[caseNames.length - 1] = "dir";
//    snapshot.addContent(FileTestUtil.getAllFilesAsString(buildDir), caseNames);
//    caseNames[caseNames.length - 1] = "package";
//    snapshot.addContent(Files.readString(buildDir.resolve(PACKAGE_JSON)), caseNames);
//  }
//
//  private static class MockRepository implements Repository {
//
//    @Override
//    public boolean retrieveDependency(Path targetPath, IDependency dependency) throws IOException
// {
//      assertEquals(NUTSHOP, dependency);
//      Files.createDirectories(targetPath);
//      Files.writeString(targetPath.resolve("package.json"), "test");
//      return true;
//    }
//
//    @Override
//    public Optional<Dependency> resolveDependency(String packageName) {
//      if (NUTSHOP.getName().equals(packageName)) {
//        return Optional.of(NUTSHOP);
//      }
//      return Optional.empty();
//    }
//
//    private static final Dependency NUTSHOP = new Dependency("datasqrl.examples.Nutshop", "0.1.0",
// "dev");
//  }
// }
