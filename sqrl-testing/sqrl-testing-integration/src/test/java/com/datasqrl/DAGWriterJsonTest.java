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

import com.datasqrl.compile.DagWriter;
import com.datasqrl.util.ArgumentsProviders;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/** Creates and snapshots a DAG plan and complete script printout for a few use cases */
public class DAGWriterJsonTest extends AbstractUseCaseTest {

  public static final List<Path> USECASE_DIRS =
      List.of(
          getResourcesDirectory("usecases/clickstream"),
          getResourcesDirectory("usecases/passthrough"),
          getResourcesDirectory("usecases/analytics-only"));

  @Override
  @ParameterizedTest
  @ArgumentsSource(UseCaseFiles.class)
  void testUseCase(Path packageFile) {
    super.testUseCase(packageFile);
  }

  @Override
  public Predicate<Path> getBuildDirFilter() {
    return path -> {
      var fileName = path.getFileName().toString();
      return fileName.endsWith(DagWriter.EXPLAIN_JSON_FILENAME)
          || fileName.endsWith(DagWriter.FULL_SOURCE_FILENAME);
    };
  }

  @Override
  public Predicate<Path> getPlanDirFilter() {
    return path -> false;
  }

  static class UseCaseFiles extends ArgumentsProviders.PackageProvider {
    public UseCaseFiles() {
      super(USECASE_DIRS);
    }
  }
}
