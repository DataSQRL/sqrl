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
package com.datasqrl.packager;

import com.datasqrl.compile.TestPlan;
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.RootPath;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.EnginePhysicalPlan.DeploymentArtifact;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.PhysicalPlan.PhysicalStagePlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.MainScript;
import com.datasqrl.util.FlinkCompileException;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.google.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;

@Getter
@AllArgsConstructor(onConstructor_ = @Inject)
public class Packager {

  private final RootPath rootDir;
  private final PackageJson config;
  private final BuildPath buildDir;
  private final FilePreprocessingPipeline preprocPipeline;
  private final MainScript mainScript;

  public void preprocess(ErrorCollector errors) {
    errors.checkFatal(
        config.getScriptConfig().getMainScript().map(StringUtils::isNotBlank).orElse(Boolean.FALSE),
        "No config or main script specified");
    try {
      cleanBuildDir(buildDir.buildDir());
      createBuildDir(buildDir.buildDir());
      preprocPipeline.run(rootDir.rootDir(), errors);
      configureScripts(buildDir.buildDir());
      writePackageConfig();
    } catch (IOException e) {
      throw errors.handle(e);
    }
  }

  @SneakyThrows
  public void postprocess(Path targetDir, PhysicalPlan plan, TestPlan testPlan) {
    Path planDir = targetDir.resolve(SqrlConstants.PLAN_DIR);
    Files.createDirectories(planDir);
    // We'll write a single asset for each folder in the physical plan stage, plus any deployment
    // artifacts that the plan has
    for (PhysicalStagePlan stagePlan : plan.getStagePlans()) {
      writePlan(stagePlan.stage().name(), stagePlan.plan(), planDir);
    }

    if (testPlan != null) {
      Path path = planDir.resolve("test.json");
      SqrlObjectMapper.INSTANCE
          .writerWithDefaultPrettyPrinter()
          .writeValue(path.toFile(), testPlan);
    }
  }

  public void postprocessFlinkCompileError(FlinkCompileException err) {
    if (StringUtils.isNotBlank(err.getDag())) {
      var dagPath = buildDir.buildDir().resolve("compile_error_dag.log");
      writeTextToFile(dagPath, err.getDag());
    }

    if (CollectionUtils.isNotEmpty(err.getFlinkSql())) {
      var sqlPath = buildDir.buildDir().resolve("compile_error_sql.log");
      writeTextToFile(sqlPath, String.join("\n", err.getFlinkSql()));
    }
  }

  /**
   * Runs templating engine with configuration values on all SQRL files
   *
   * @param buildDir the build directory to process
   * @throws IOException if file operations fail
   */
  private void configureScripts(Path buildDir) throws IOException {
    Map<String, Object> templateValues = config.getScriptConfig().getConfig();
    if (!templateValues.isEmpty()) {
      MustacheFactory mustacheFactory = new DefaultMustacheFactory();
      try (var pathStream = Files.walk(buildDir)) {
        pathStream
            .filter(Files::isRegularFile)
            .filter(path -> path.toString().endsWith(".sqrl"))
            .forEach(
                sqrlFile -> {
                  try {
                    String content = Files.readString(sqrlFile);
                    Mustache mustache =
                        mustacheFactory.compile(
                            new StringReader(content), sqrlFile.getFileName().toString());
                    StringWriter writer = new StringWriter();
                    mustache.execute(writer, templateValues);
                    Files.writeString(
                        sqrlFile, writer.toString(), StandardOpenOption.TRUNCATE_EXISTING);
                  } catch (Exception e) {
                    throw new RuntimeException("Failed to template SQRL file: " + sqrlFile, e);
                  }
                });
      }
    }
  }

  private void writePackageConfig() throws IOException {
    config.toFile(buildDir.buildDir().resolve(SqrlConstants.PACKAGE_JSON), true);
  }

  @SneakyThrows
  private static void createBuildDir(Path buildDir) {
    Files.createDirectories(buildDir);
  }

  private static void cleanBuildDir(Path buildDir) throws IOException {
    if (Files.exists(buildDir) && Files.isDirectory(buildDir)) {
      // Sort the paths in reverse order so that directories are deleted last
      try (var files = Files.walk(buildDir)) {
        files.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
    } else if (Files.exists(buildDir) && !Files.isDirectory(buildDir)) {
      buildDir.toFile().delete();
    }
  }

  @SneakyThrows
  private void writePlan(String name, EnginePhysicalPlan plan, Path planDir) {
    Files.createDirectories(planDir);

    DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
    prettyPrinter.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE);
    ObjectWriter jsonWriter =
        SqrlObjectMapper.INSTANCE.enable(SerializationFeature.INDENT_OUTPUT).writer(prettyPrinter);

    var artifacts =
        ListUtils.union(
            plan.getDeploymentArtifacts(), List.of(new DeploymentArtifact(".json", plan)));
    for (var artifact : artifacts) {
      var filePath = planDir.resolve(name + artifact.fileSuffix()).toAbsolutePath();
      if (artifact.content() instanceof String content) {
        writeTextToFile(filePath, content);
      } else { // serialize as json
        jsonWriter.writeValue(filePath.toFile(), plan);
      }
    }
  }

  @SneakyThrows
  private void writeTextToFile(Path path, String content) {
    Files.writeString(
        path,
        content,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE);
  }
}
