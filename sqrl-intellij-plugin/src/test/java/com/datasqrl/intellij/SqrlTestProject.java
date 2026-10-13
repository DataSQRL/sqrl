/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.intellij;

import com.datasqrl.intellij.SqrlSettings.Launcher;
import com.intellij.codeInsight.daemon.impl.HighlightInfo;
import com.intellij.lang.annotation.HighlightSeverity;
import com.intellij.openapi.module.ModuleTypeManager;
import com.intellij.openapi.vfs.LocalFileSystem;
import com.intellij.openapi.vfs.VirtualFile;
import com.intellij.testFramework.PsiTestUtil;
import com.intellij.testFramework.fixtures.CodeInsightTestFixture;
import com.intellij.testFramework.fixtures.impl.CodeInsightTestFixtureImpl;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * The project of the CLI tests on disk, whose scripts the language server checks when it runs from
 * the jar of the CLI, which the build passes as {@code sqrl.cli.jar}.
 */
final class SqrlTestProject {

  static final long TIMEOUT_SECONDS = 120;

  /** The main script, with an unknown column */
  static final String SCRIPT =
      """
      IMPORT shop.sources;

      OrderStream := SELECT * FROM sources.Orders;
      Orders := DISTINCT OrderStream ON id ORDER BY updated_at DESC;
      CustomerTotals := SELECT customer_id, SUM(amout) AS total FROM Orders GROUP BY customer_id;
      """;

  private SqrlTestProject() {}

  /** Starts the language server from the jar of the CLI, unless it was not built. */
  static boolean useCliJar() {
    var jar = System.getProperty("sqrl.cli.jar", "");
    if (!Files.isRegularFile(Path.of(jar))) {
      System.err.println("Skipped, the jar of the CLI was not built: " + jar);
      return false;
    }
    var settings = SqrlSettings.getInstance().getState();
    settings.launcher = Launcher.JAR;
    settings.jarPath = jar;
    return true;
  }

  /**
   * Creates the project in the directory of the fixture, and adds it to the IDE project, since
   * language servers only start for the files of the IDE project.
   *
   * @return the main script
   */
  static VirtualFile create(CodeInsightTestFixture fixture) throws IOException {
    var main = createFiles(fixture);
    var root = LocalFileSystem.getInstance().refreshAndFindFileByPath(fixture.getTempDirPath());
    PsiTestUtil.addModule(
        fixture.getProject(), ModuleTypeManager.getInstance().getDefaultModuleType(), "sqrl", root);
    return main;
  }

  /** The error of the open script in the editor, once the language server checked it. */
  static Optional<HighlightInfo> error(CodeInsightTestFixture fixture) {
    // The language server restarts the highlighting when it publishes diagnostics
    return CodeInsightTestFixtureImpl.instantiateAndRun(
            fixture.getFile(), fixture.getEditor(), new int[0], true)
        .stream()
        .filter(info -> info.getSeverity() == HighlightSeverity.ERROR)
        .filter(info -> info.getDescription().contains("Column 'amout' not found"))
        .findFirst();
  }

  /** A main script that imports a script of a package. */
  private static VirtualFile createFiles(CodeInsightTestFixture fixture) throws IOException {
    var files = fixture.getTempDirFixture();
    files.createFile(
        "shared/package.json",
        "{\"version\": \"1\", \"script\": {\"config\": {\"interval\": \"10 sec\"}}}");
    files.createFile(
        "shared/orders.jsonl",
        """
        {"id": 1, "customer_id": 10, "amount": 12.5, "updated_at": "2024-01-01T10:00:00.000Z"}
        {"id": 2, "customer_id": 11, "amount": 7.0, "updated_at": "2024-01-01T11:00:00.000Z"}
        """);
    files.createFile(
        "shared/sources.sqrl",
        """
        CREATE TABLE Orders (
          PRIMARY KEY (id, `updated_at`) NOT ENFORCED,
          WATERMARK FOR `updated_at` AS `updated_at` - INTERVAL '0.001' SECOND
        ) WITH (
          'source.monitor-interval' = '{{interval}}'
        )
        LIKE `orders.jsonl`;
        """);
    files.createFile(
        "project/package.json",
        """
        {
          "version": "1",
          "enabled-engines": ["vertx", "postgres", "kafka", "flink"],
          "script": {
            "main": "main.sqrl",
            "config": {"variant": "test"},
            "include": {"shop": {"package": "../shared/package.json"}}
          }
        }
        """);
    return files.createFile("project/main.sqrl", SCRIPT);
  }
}
