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
package com.datasqrl.packager.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class GeneratePackageIdTest {

  @Test
  void generateIds() {
    Set<String> ids = new HashSet<>();
    for (var i = 0; i < 100000; i++) {
      var id = GeneratePackageId.generate();
      assertThat(id).hasSize(27);
      assertThat(ids.add(id)).isTrue();
    }
  }

  @Test
  @Disabled
  void generateSingleId() {
    System.out.println(GeneratePackageId.generate());
  }
}
