/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.datasqrl.io.schema.flexible.type.basic.BasicType;
import com.datasqrl.io.schema.flexible.type.basic.BasicTypeManager;
import com.datasqrl.io.schema.flexible.type.basic.BigIntType;
import com.datasqrl.io.schema.flexible.type.basic.DoubleType;
import com.datasqrl.io.schema.flexible.type.basic.IntervalType;
import com.datasqrl.io.schema.flexible.type.basic.StringType;
import com.datasqrl.io.schema.flexible.type.basic.TimestampType;
import com.datasqrl.util.SnapshotTest;

public class TypeTest {

  SnapshotTest.Snapshot snapshot;

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
  }

  @Test
  public void printCombinationMatrix() {
    for (Map.Entry<Pair<BasicType, BasicType>, Pair<BasicType, Integer>> entry : BasicTypeManager.TYPE_COMBINATION_MATRIX.entrySet()) {
      var types = entry.getKey();
      var result = entry.getValue();
      snapshot.addContent("%s [%d]".formatted(result.getKey(), result.getValue()),
          "%s + %s".formatted(types.getKey(), types.getValue()));
    }
    snapshot.createOrValidate();
  }

  @Test
  public void testBasicTypes() {
    assertEquals(6, BasicTypeManager.ALL_TYPES.length);
    assertEquals(DoubleType.INSTANCE,
        BasicTypeManager.combine(BigIntType.INSTANCE, DoubleType.INSTANCE, 10).get());
    assertEquals(StringType.INSTANCE,
        BasicTypeManager.combine(BigIntType.INSTANCE, TimestampType.INSTANCE, 50).get());
    assertEquals(IntervalType.INSTANCE,
        BasicTypeManager.combine(BigIntType.INSTANCE, IntervalType.INSTANCE, 40).get());
  }

}
