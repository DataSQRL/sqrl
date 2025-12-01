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
package com.datasqrl.cli;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ResourceUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@CommandLine.Command(
    name = "add-udf",
    description =
        "Adds a new user-defined function into the 'functions' folder of an existing project")
public class AddUdfCmd extends BaseCmd {

  private static final String UDF_TEMPLATE_DIR = "templates/udfs";
  private static final String AGGREGATE_UDF_TEMPLATE = UDF_TEMPLATE_DIR + "/aggregate.java";
  private static final String SCALAR_UDF_TEMPLATE = UDF_TEMPLATE_DIR + "/scalar.java";
  private static final String UDF_NAME_PLACEHOLDER = "__udfname__";

  @Parameters(
      index = "0",
      description = "Name of the newly added user-defined Java file and function class")
  String udfName;

  @Option(
      names = {"--aggregate"},
      description = "Add an aggregate UDF function instead of a scalar one")
  boolean aggregate = false;

  @Override
  protected void runInternal(ErrorCollector errors) {
    try {
      addUdf(() -> cli.rootDir);
    } catch (Exception e) {
      throw errors.exception("Failed to add UDF %s: %s", udfName, e);
    }
  }

  @SneakyThrows
  void addUdf(Supplier<Path> targetRoot) {
    var resource = aggregate ? AGGREGATE_UDF_TEMPLATE : SCALAR_UDF_TEMPLATE;

    try (var is = ResourceUtils.getResourceAsStream(resource)) {
      var targetPath = targetRoot.get().resolve("functions").resolve(udfName + ".java");
      Files.createDirectories(targetPath.getParent());

      var content = IOUtils.toString(is, UTF_8).replaceAll(UDF_NAME_PLACEHOLDER, udfName);
      Files.writeString(targetPath, content, StandardOpenOption.CREATE_NEW);
    }
  }
}
