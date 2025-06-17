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

import java.nio.file.Path;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class ExternalUseCaseCompileTest extends AbstractUseCaseTest {

  public static Path USECASE_DIR;

  @Test
  public void testIndividual() {
    super.testUsecase(
        Path.of("/Users/matthias/git/datasqrl-cba/fraud/fraud.sqrl"),
        null,
        Path.of("/Users/matthias/git/datasqrl-cba/fraud/fraud_package_test.json"));
  }
}
