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
package com.datasqrl;

import com.datasqrl.tests.DuckdbTestExtension;
import com.datasqrl.tests.IcebergTestExtension;
import com.datasqrl.tests.SnowflakeTestExtension;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests external use cases manually, provided via {@code externalUseCaseProvider}. */
@Slf4j
@Disabled
@ExtendWith({DuckdbTestExtension.class, IcebergTestExtension.class, SnowflakeTestExtension.class})
public class ExternalUseCaseIT extends AbstractFullUseCaseTest {

  @ParameterizedTest
  @MethodSource("externalUseCaseProvider")
  void testCase(UseCaseParam param) {
    fullUseCaseTest(param);
  }

  static Stream<UseCaseParam> externalUseCaseProvider() {
    // Provide absolute paths for the package.json file of the external project(s) to test
    Stream<Path> pathStream =
        Stream.of(
            // Path.of("<absolute-path-of-external-package-json>")
            );

    var partitionedPaths = pathStream.collect(Collectors.partitioningBy(Files::exists));
    log.warn(
        "The following path point to a non-existing file, and will be ignored: {}",
        partitionedPaths.get(false));

    var existingPaths = partitionedPaths.get(true);

    return existingPaths.stream().map(UseCaseParam::new);
  }
}
