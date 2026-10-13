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
package com.datasqrl.lsp;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.lsp.FunctionSources.Import;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.eclipse.lsp4j.Position;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FunctionSourcesTest {

  @TempDir Path dir;

  @Test
  void givenImportStatements_whenAt_thenFindsWhatTheyImport() {
    var text =
        """
        IMPORT udfs.read_partition_sizes;
        IMPORT udfs.MyFunction AS fn;
        X := SELECT 1 AS one;
        """;

    assertThat(FunctionSources.at(text, 0, "IMPORT udfs.read".length()))
        .contains(new Import("read_partition_sizes", List.of("udfs", "read_partition_sizes")));
    assertThat(FunctionSources.at(text, 1, "IMPORT udfs.MyFunction AS f".length()))
        .contains(new Import("fn", List.of("udfs", "MyFunction")));
    assertThat(FunctionSources.at(text, 0, "IMP".length())).isEmpty();
    assertThat(FunctionSources.at(text, 2, 1)).isEmpty();
  }

  @Test
  void givenJarWithItsSourcesBesideIt_whenFind_thenFindsTheSourceAndNotItsCopyInTheBuild()
      throws IOException {
    var source = write("udfs/src/main/java/com/myudf/read_partition_sizes.java");
    write("build/udfs/src/main/java/com/myudf/read_partition_sizes.java");

    assertThat(FunctionSources.find(dir, dir.resolve("udfs"), "com.myudf.read_partition_sizes"))
        .contains(source);
  }

  @Test
  void givenJarImportedFromTheTargetOfItsProject_whenFind_thenPrefersTheSourceOfThatProject()
      throws IOException {
    write("other/src/main/java/com/myudf/MyScalarFunction.java");
    var source = write("myjavafunction/src/main/java/com/myudf/MyScalarFunction.java");

    assertThat(
            FunctionSources.find(
                dir, dir.resolve("myjavafunction/target"), "com.myudf.MyScalarFunction"))
        .contains(source);
  }

  @Test
  void givenJBangSourceWithoutDirectoriesOfItsPackage_whenFind_thenFindsItByName()
      throws IOException {
    var source = write("usrlib/MyScalarFunction.java");

    assertThat(FunctionSources.find(dir, dir.resolve("usrlib"), "MyScalarFunction"))
        .contains(source);
    assertThat(FunctionSources.find(dir, dir.resolve("usrlib"), "com.example.MyScalarFunction"))
        .contains(source);
    assertThat(FunctionSources.find(dir, dir.resolve("usrlib"), "com.example.Other")).isEmpty();
  }

  @Test
  void givenSource_whenDeclaration_thenIsAtTheNameOfTheClass() {
    var source =
        """
        package com.myudf;

        /** A class that is mentioned in a comment */
        public class MyScalarFunction extends ScalarFunction {
        """;

    assertThat(FunctionSources.declaration(source, "com.myudf.MyScalarFunction"))
        .isEqualTo(new Position(3, "public class ".length()));
  }

  private Path write(String path) throws IOException {
    var file = dir.resolve(path);
    Files.createDirectories(file.getParent());
    return Files.writeString(file, "public class " + file.getFileName() + " {}\n");
  }
}
