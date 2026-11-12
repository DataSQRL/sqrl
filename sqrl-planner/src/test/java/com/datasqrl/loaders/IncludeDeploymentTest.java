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
package com.datasqrl.loaders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.TestScriptConfig;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.loaders.IncludeDeployment.ScriptPlanning;
import com.datasqrl.loaders.resolver.FileResourceResolver;
import com.datasqrl.planner.parser.SqrlStatementParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IncludeDeploymentTest {

  private final SqrlStatementParser parser = new SqrlStatementParser();

  @TempDir Path buildDir;
  // The included project is built into the directory of its namespace
  private Path includeDir;
  private WorkspacePaths workspacePaths;

  @BeforeEach
  void setUp() throws IOException {
    includeDir = buildDir.resolve("shared");
    workspacePaths = new WorkspacePaths(buildDir, buildDir, buildDir.resolve("target"), buildDir);
    write("main.sqrl", "IMPORT inline.*;\nIMPORT namespaced;\nIMPORT tables.Orders;\n");
    write("inline.sqrl", "IMPORT sub.nested.*;\nIMPORT missing.*;\n");
    write("sub/nested.sqrl", "IMPORT root.rooted.*;\n");
    write("rooted.sqrl", "IMPORT tables.Orders;\n");
    write("namespaced.sqrl", "IMPORT below.*;\n");
    write("below.sqrl", "IMPORT tables.Orders;\n");
    write(
        "tables.sqrl",
        "CREATE TABLE Orders (id BIGINT NOT NULL) WITH ('connector' = 'datagen');\n");
    write("unplanned.sqrl", "IMPORT inline.*;\n");
  }

  @Test
  void givenMainScript_whenAnalyzed_thenInlineImportsAtTheRootArePlannedAtTheRoot() {
    var deployment = analyze("main.sqrl");

    assertThat(planning(deployment, "main.sqrl")).isEqualTo(ScriptPlanning.PLANNED_AT_ROOT);
    assertThat(planning(deployment, "inline.sqrl")).isEqualTo(ScriptPlanning.PLANNED_AT_ROOT);
    // Imported inline from a subdirectory, which imports a script from the root directory inline
    assertThat(planning(deployment, "sub/nested.sqrl")).isEqualTo(ScriptPlanning.PLANNED_AT_ROOT);
    assertThat(planning(deployment, "rooted.sqrl")).isEqualTo(ScriptPlanning.PLANNED_AT_ROOT);
  }

  @Test
  void givenMainScript_whenAnalyzed_thenNamespacedImportsAndTheirImportsAreOnlyPlanned() {
    var deployment = analyze("main.sqrl");

    assertThat(planning(deployment, "namespaced.sqrl")).isEqualTo(ScriptPlanning.PLANNED);
    // Imported inline, but from a script that is not at the root
    assertThat(planning(deployment, "below.sqrl")).isEqualTo(ScriptPlanning.PLANNED);
  }

  @Test
  void givenMainScript_whenAnalyzed_thenScriptsThatItDoesNotImportAreNotPlanned() {
    var deployment = analyze("main.sqrl");

    // Importing a single table does not plan the script that defines it
    assertThat(planning(deployment, "tables.sqrl")).isEqualTo(ScriptPlanning.NOT_PLANNED);
    assertThat(planning(deployment, "unplanned.sqrl")).isEqualTo(ScriptPlanning.NOT_PLANNED);
  }

  @Test
  void givenMissingMainScript_whenAnalyzed_thenNoScriptIsPlanned() {
    var deployment = analyze("absent.sqrl");

    assertThat(planning(deployment, "main.sqrl")).isEqualTo(ScriptPlanning.NOT_PLANNED);
  }

  @Test
  void givenPlannings_whenChecked_thenOnlyTheRootExposesSubscriptions() {
    assertThat(ScriptPlanning.PLANNED_AT_ROOT.exposesSubscriptions()).isTrue();
    assertThat(ScriptPlanning.PLANNED_AT_ROOT.runsStatements()).isTrue();
    assertThat(ScriptPlanning.PLANNED.exposesSubscriptions()).isFalse();
    assertThat(ScriptPlanning.PLANNED.runsStatements()).isTrue();
    assertThat(ScriptPlanning.NOT_PLANNED.runsStatements()).isFalse();
  }

  @Test
  void givenStandaloneIncludeWithMainScript_whenGetIncludedScript_thenUsesItsDeployment()
      throws IOException {
    write("package.json", "{ \"script\": { \"main\": \"main.sqrl\" } }");
    var errors = ErrorCollector.root();
    var moduleLoaders = moduleLoaders(errors);

    var inline = moduleLoaders.getIncludedScript(includeDir.resolve("inline.sqrl"));
    var namespaced = moduleLoaders.getIncludedScript(includeDir.resolve("namespaced.sqrl"));

    assertThat(inline)
        .hasValueSatisfying(
            script -> {
              assertThat(script.include()).isEqualTo("shared");
              assertThat(script.exposesSubscriptions()).isTrue();
            });
    assertThat(namespaced)
        .hasValueSatisfying(
            script -> {
              assertThat(script.exposesSubscriptions()).isFalse();
              assertThat(script.runsStatements()).isTrue();
            });
    assertThat(errors.getErrors()).isEmpty();
  }

  @Test
  void givenStandaloneIncludeWithoutMainScript_whenGetIncludedScript_thenWarnsAndPlansNothing()
      throws IOException {
    write("package.json", "{ \"version\": \"1\" }");
    var errors = ErrorCollector.root();
    var moduleLoaders = moduleLoaders(errors);

    var script = moduleLoaders.getIncludedScript(includeDir.resolve("main.sqrl"));

    assertThat(script).hasValueSatisfying(s -> assertThat(s.runsStatements()).isFalse());
    assertThat(errors.getErrors())
        .filteredOn(ErrorMessage::isWarning)
        .extracting(ErrorMessage::getMessage)
        .singleElement()
        .asString()
        .contains("'shared'", "\"script.main\"");
  }

  private ModuleLoaders moduleLoaders(ErrorCollector errors) {
    var packageJson = mock(PackageJson.class);
    when(packageJson.getScriptConfig())
        .thenReturn(TestScriptConfig.withIncludes(Map.of("shared", true)));
    return new ModuleLoaders(
        packageJson,
        new FileResourceResolver(buildDir),
        parser,
        workspacePaths,
        new ClasspathFunctionLoader(),
        new UdfJarClassLoaders(),
        errors);
  }

  private IncludeDeployment analyze(String mainScript) {
    var loader =
        new ModuleLoaderImpl(
            new FileResourceResolver(includeDir),
            parser,
            workspacePaths,
            new ClasspathFunctionLoader(),
            new UdfJarClassLoaders(),
            ErrorCollector.root());
    return IncludeDeployment.analyze(loader, mainScript, parser);
  }

  private ScriptPlanning planning(IncludeDeployment deployment, String script) {
    return deployment.getPlanning(includeDir.resolve(script));
  }

  private void write(String file, String content) throws IOException {
    var path = includeDir.resolve(file);
    Files.createDirectories(path.getParent());
    Files.writeString(path, content);
  }
}
