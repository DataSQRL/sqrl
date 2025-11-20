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

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.util.SnapshotTest.Snapshot;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class EngineValidationTest extends AbstractAssetSnapshotTest {

  public static final Path PROJECT_DIR = getResourcesDirectory("engine-validation");

  protected EngineValidationTest() {
    super(PROJECT_DIR.resolve("plan-output"));
  }

  @Test
  void testInvalidEngine() {
    var pkg = PROJECT_DIR.resolve("package-fail.json");
    assertThat(pkg).isRegularFile();

    var testModifier = TestNameModifier.of(pkg);
    var expectFailure = testModifier == TestNameModifier.fail;
    var printMessages =
        testModifier == TestNameModifier.fail || testModifier == TestNameModifier.warn;
    this.snapshot = Snapshot.of(getDisplayName(pkg), getClass());
    var hook =
        execute(
            PROJECT_DIR,
            "compile",
            pkg.getFileName().toString(),
            "-t",
            outputDir.getFileName().toString());
    assertThat(hook.isFailed()).as(hook.getMessages()).isEqualTo(expectFailure);
    if (printMessages) {
      createMessageSnapshot(hook.getMessages());
    } else {
      createSnapshot();
    }
  }
}
