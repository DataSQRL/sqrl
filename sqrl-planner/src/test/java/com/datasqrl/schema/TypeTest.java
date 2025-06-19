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
package com.datasqrl.schema;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.io.schema.flexible.type.basic.BasicType;
import com.datasqrl.io.schema.flexible.type.basic.BasicTypeManager;
import com.datasqrl.io.schema.flexible.type.basic.BigIntType;
import com.datasqrl.io.schema.flexible.type.basic.DoubleType;
import com.datasqrl.io.schema.flexible.type.basic.IntervalType;
import com.datasqrl.io.schema.flexible.type.basic.StringType;
import com.datasqrl.io.schema.flexible.type.basic.TimestampType;
import com.datasqrl.util.SnapshotTest;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class TypeTest {

  SnapshotTest.Snapshot snapshot;

  @BeforeEach
  void setup(TestInfo testInfo) throws IOException {
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
  }

  @Test
  void printCombinationMatrix() {
    for (Map.Entry<Pair<BasicType, BasicType>, Pair<BasicType, Integer>> entry :
        BasicTypeManager.TYPE_COMBINATION_MATRIX.entrySet()) {
      var types = entry.getKey();
      var result = entry.getValue();
      snapshot.addContent(
          "%s [%d]".formatted(result.getKey(), result.getValue()),
          "%s + %s".formatted(types.getKey(), types.getValue()));
    }
    snapshot.createOrValidate();
  }

  @Test
  void basicTypes() {
    assertThat(BasicTypeManager.ALL_TYPES.length).isEqualTo(6);
    assertThat(BasicTypeManager.combine(BigIntType.INSTANCE, DoubleType.INSTANCE, 10).get())
        .isEqualTo(DoubleType.INSTANCE);
    assertThat(BasicTypeManager.combine(BigIntType.INSTANCE, TimestampType.INSTANCE, 50).get())
        .isEqualTo(StringType.INSTANCE);
    assertThat(BasicTypeManager.combine(BigIntType.INSTANCE, IntervalType.INSTANCE, 40).get())
        .isEqualTo(IntervalType.INSTANCE);
  }
}
