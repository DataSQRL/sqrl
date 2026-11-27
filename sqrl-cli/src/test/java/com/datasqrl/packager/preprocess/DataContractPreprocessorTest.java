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
package com.datasqrl.packager.preprocess;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.packager.FilePreprocessingPipeline;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DataContractPreprocessorTest {

  @TempDir Path tempDir;

  private final ErrorCollector errors = ErrorCollector.root();
  private Path projectDir;
  private Path buildDir;
  private DataContractPreprocessor underTest;

  @BeforeEach
  void setUp() {
    projectDir = tempDir.resolve("project");
    buildDir = projectDir.resolve("build");
    underTest =
        new DataContractPreprocessor(
            new WorkspacePaths(projectDir, buildDir, buildDir.resolve("deploy"), projectDir),
            errors);
  }

  @Test
  void givenContractsNextToTheirScripts_whenProcess_thenCopiesThemToTheBuildFolder() {
    // The walk can pass the contract before the script
    process(file("connectors/crm.odcs.yaml"));
    process(file("connectors/crm.sqrl"));
    process(file("orders.table.sql"));
    process(file("orders.odcs.yaml"));
    // A name that differs in case pairs here, and the planner reports it where it loads contracts
    process(file("Shipping.odcs.yaml"));
    process(file("shipping.sqrl"));

    assertThatCode(underTest::complete).doesNotThrowAnyException();

    assertThat(buildDir.resolve("connectors/crm.odcs.yaml")).isRegularFile();
    assertThat(buildDir.resolve("orders.odcs.yaml")).isRegularFile();
    assertThat(buildDir.resolve("Shipping.odcs.yaml")).isRegularFile();
    assertThat(errors.hasErrorsWarningsOrNotices()).isFalse();
  }

  @Test
  void givenContractExtensionInAnotherCase_whenProcess_thenTheFileIsNotIgnored() {
    // With a script the file is copied, and the planner reports that its name differs in case
    process(file("orders.ODCS.YAML"));
    process(file("orders.sqrl"));
    // Without a script it is reported here, like any contract file
    process(file("connectors/crm.Odcs.Yaml"));

    assertThatThrownBy(underTest::complete).isInstanceOf(CollectedException.class);

    assertThat(buildDir.resolve("orders.ODCS.YAML")).isRegularFile();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .containsExactly(
            "Contract file [connectors/crm.Odcs.Yaml] has no SQRL file next to it. A contract"
                + " applies to the tables of the script with the same name: expected crm.sqrl or"
                + " crm.table.sql in the same folder");
  }

  @Test
  void givenContractWithoutScript_whenComplete_thenReportsItWithItsFolderAndFails() {
    process(file("connectors/crm.odcs.yaml"));
    process(file("connectors/crm.graphqls"));
    process(file("crm.sqrl"));

    assertThatThrownBy(underTest::complete).isInstanceOf(CollectedException.class);

    assertThat(messages())
        .extracting(ErrorMessage::getMessage, ErrorMessage::getErrorLabel)
        .containsExactly(
            tuple(
                "Contract file [connectors/crm.odcs.yaml] has no SQRL file next to it. A contract"
                    + " applies to the tables of the script with the same name: expected crm.sqrl"
                    + " or crm.table.sql in the same folder",
                ErrorCode.INVALID_DATA_CONTRACT));
  }

  @Test
  void givenFilesWithExtensionsThatAreNotRead_whenComplete_thenReportsAllOfThemAndFails() {
    process(file("connectors/legacy.odcs.yml"));
    process(file("sinks/legacy.odcs.yml"));
    process(file("exported.ODCS.json"));
    process(file("connectors/legacy.sqrl"));

    assertThatThrownBy(underTest::complete).isInstanceOf(CollectedException.class);

    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .containsExactly(
            "File [connectors/legacy.odcs.yml] looks like a data contract but is not read. Rename"
                + " it to [legacy.odcs.yaml]",
            "File [exported.ODCS.json] looks like a data contract but is not read. Rename it to"
                + " [exported.odcs.yaml]",
            "File [sinks/legacy.odcs.yml] looks like a data contract but is not read. Rename it to"
                + " [legacy.odcs.yaml]");
    assertThat(buildDir).doesNotExist();
  }

  @Test
  void givenFilesVisitedInAnotherOrder_whenComplete_thenReportsThemInTheSameOrder() {
    // The order in which a folder is listed depends on the file system
    var names = List.of("orphan.odcs.yaml", "legacy.odcs.yml", "exported.odcs.json");

    names.forEach(name -> process(file(name)));
    assertThatThrownBy(underTest::complete).isInstanceOf(CollectedException.class);
    var firstRun = messages().stream().map(ErrorMessage::getMessage).toList();

    names.stream().sorted().forEach(name -> process(file(name)));
    assertThatThrownBy(underTest::complete).isInstanceOf(CollectedException.class);
    var secondRun =
        messages().stream().skip(firstRun.size()).map(ErrorMessage::getMessage).toList();

    assertThat(firstRun)
        .extracting(message -> message.substring(0, message.indexOf(']') + 1))
        .containsExactly(
            "File [exported.odcs.json]",
            "File [legacy.odcs.yml]",
            "Contract file [orphan.odcs.yaml]");
    assertThat(secondRun).isEqualTo(firstRun);
  }

  @Test
  void givenContractsThatAnEarlierCompilationWrote_whenProcess_thenTheyAreNotOfTheProject() {
    // A target folder in the project, and the build folder of an included project
    file("artifacts/plan/flink-sql.sql");
    process(file("artifacts/contracts/sources.odcs.yaml"));
    file("shared/build/deploy/plan/kafka.json");
    process(file("shared/build/deploy/contracts/sinks.odcs.yaml"));

    assertThatCode(underTest::complete).doesNotThrowAnyException();

    assertThat(buildDir).doesNotExist();
    assertThat(errors.hasErrorsWarningsOrNotices()).isFalse();
  }

  @Test
  void givenRunThatFailed_whenNextRunCompletes_thenItStartsWithoutTheFilesOfTheFirst() {
    process(file("included/crm.odcs.yaml"));
    assertThatThrownBy(underTest::complete).isInstanceOf(CollectedException.class);
    var reported = messages().size();

    process(file("crm.sqrl"));
    process(file("crm.odcs.yaml"));

    // The error of the first run is still recorded, and this run reports none
    assertThatCode(underTest::complete).doesNotThrowAnyException();
    assertThat(messages()).hasSize(reported);
  }

  /** Processes the file in the context of its folder, as the pipeline does. */
  private void process(Path file) {
    var folder = file.getParent();
    underTest.process(
        file,
        new FilePreprocessingPipeline.Context(
            folder,
            buildDir.resolve(projectDir.relativize(folder)),
            buildDir.resolve("lib"),
            buildDir.resolve("data"),
            Optional.empty(),
            errors));
  }

  @SneakyThrows
  private Path file(String name) {
    var path = projectDir.resolve(name);
    Files.createDirectories(path.getParent());
    return Files.writeString(path, "");
  }

  private List<ErrorMessage> messages() {
    return StreamSupport.stream(errors.spliterator(), false).toList();
  }
}
