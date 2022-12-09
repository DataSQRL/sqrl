/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager;

import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestScript;
import com.datasqrl.util.data.Retail;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
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

  @SneakyThrows
  private void testCombination(Path main, Path graphQl, Path packageFile) {
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
    Packager pkg = builder.build().getPackager();
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


}
