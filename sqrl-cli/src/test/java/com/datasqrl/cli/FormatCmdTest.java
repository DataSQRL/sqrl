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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FormatCmdTest {

  private static final String UNFORMATTED = "X := SELECT\nid\nFROM t;\n";
  private static final String FORMATTED = "X := SELECT\n    id\nFROM t;\n";

  @TempDir private Path workspace;

  private final ByteArrayOutputStream output = new ByteArrayOutputStream();

  @BeforeEach
  void createScripts() throws IOException {
    Files.writeString(workspace.resolve("a.sqrl"), UNFORMATTED);
    Files.createDirectories(workspace.resolve("lib"));
    Files.writeString(workspace.resolve("lib/b.sqrl"), FORMATTED);
    Files.createDirectories(workspace.resolve("build"));
    Files.writeString(workspace.resolve("build/c.sqrl"), UNFORMATTED);
  }

  private int format(String... args) {
    var cli = new DatasqrlCli(workspace);
    var commandLine = cli.getCmd();
    commandLine.getSubcommands().get("format").<FormatCmd>getCommand().out =
        new PrintStream(output, true);
    var arguments = new String[args.length + 1];
    arguments[0] = "format";
    System.arraycopy(args, 0, arguments, 1, args.length);
    return commandLine.execute(arguments);
  }

  @Test
  void givenUnformattedScripts_whenFormat_thenFormatsThemOutsideBuildDirectories()
      throws IOException {
    assertThat(format()).isZero();

    assertThat(workspace.resolve("a.sqrl")).hasContent(FORMATTED);
    assertThat(workspace.resolve("lib/b.sqrl")).hasContent(FORMATTED);
    assertThat(workspace.resolve("build/c.sqrl")).hasContent(UNFORMATTED);
    assertThat(output.toString()).isEqualTo("Formatted a.sqrl\n");
  }

  @Test
  void givenUnformattedScript_whenFormatCheck_thenListsItAndFails() {
    assertThat(format("--check")).isOne();

    assertThat(workspace.resolve("a.sqrl")).hasContent(UNFORMATTED);
    assertThat(output.toString())
        .isEqualTo(
            "a.sqrl:2: not formatted\n1 script(s) not formatted, run `sqrl format` to format them\n");
  }

  @Test
  void givenFormattedScript_whenFormatCheckThatScript_thenSucceeds() {
    assertThat(format("--check", "lib/b.sqrl")).isZero();

    assertThat(output.toString()).isEmpty();
  }

  @Test
  void givenUnterminatedString_whenFormat_thenReportsItAndFails() throws IOException {
    Files.writeString(workspace.resolve("lib/d.sqrl"), "X := SELECT 'open\n");

    assertThat(format("lib")).isOne();

    assertThat(output.toString())
        .isEqualTo("lib/d.sqrl: could not be formatted, e.g. a string is not terminated\n");
  }
}
