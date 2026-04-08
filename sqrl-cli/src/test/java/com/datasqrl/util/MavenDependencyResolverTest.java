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
package com.datasqrl.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class MavenDependencyResolverTest {

  private final MavenDependencyResolver underTest = new MavenDependencyResolver();

  @Test
  void given_sourceWithJdeps_when_parseJdeps_then_extractsCoordinates() {
    var source =
        """
        //JDEPS com.google.code.gson:gson:2.11.0
        //JDEPS org.apache.commons:commons-lang3:3.14.0
        import org.apache.flink.table.functions.ScalarFunction;

        public class MyUDF extends ScalarFunction {
        }
        """;

    var result = underTest.parseJdeps(source);

    assertThat(result)
        .containsExactly(
            "com.google.code.gson:gson:2.11.0", "org.apache.commons:commons-lang3:3.14.0");
  }

  @Test
  void given_sourceWithoutJdeps_when_parseJdeps_then_returnsEmptyList() {
    var source =
        """
        import org.apache.flink.table.functions.ScalarFunction;

        public class MyUDF extends ScalarFunction {
        }
        """;

    var result = underTest.parseJdeps(source);

    assertThat(result).isEmpty();
  }

  @Test
  void given_sourceWithOldDepsComment_when_parseJdeps_then_ignoresOldFormat() {
    var source =
        """
        //DEPS com.google.code.gson:gson:2.11.0
        public class MyUDF extends ScalarFunction {}
        """;

    var result = underTest.parseJdeps(source);

    assertThat(result).isEmpty();
  }

  @Test
  void given_sourceWithFlinkJdeps_when_validateNoFlinkJdeps_then_throwsError() {
    var source =
        """
        //JDEPS org.apache.flink:flink-table-common:2.2.0
        public class MyUDF extends ScalarFunction {}
        """;

    assertThatThrownBy(() -> underTest.validateNoFlinkJdeps(Path.of("MyUDF.java"), source))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Flink dependencies are provided automatically");
  }

  @Test
  void given_sourceWithNonFlinkJdeps_when_validateNoFlinkJdeps_then_passes() {
    var source =
        """
        //JDEPS com.google.code.gson:gson:2.11.0
        public class MyUDF extends ScalarFunction {}
        """;

    underTest.validateNoFlinkJdeps(Path.of("MyUDF.java"), source);
  }

  @Test
  void given_emptyCoordinates_when_resolve_then_returnsEmptyList() {
    var result = underTest.resolve(List.of());

    assertThat(result).isEmpty();
  }
}
