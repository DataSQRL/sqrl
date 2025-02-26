/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.schema.type.basic.*;
import com.datasqrl.util.SnapshotTest;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class TypeTest {

  SnapshotTest.Snapshot snapshot;

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
  }

  @Test
  public void printCombinationMatrix() {
    for (Map.Entry<Pair<BasicType, BasicType>, Pair<BasicType, Integer>> entry :
        BasicTypeManager.TYPE_COMBINATION_MATRIX.entrySet()) {
      Pair<BasicType, BasicType> types = entry.getKey();
      Pair<BasicType, Integer> result = entry.getValue();
      snapshot.addContent(
          String.format("%s [%d]", result.getKey(), result.getValue()),
          String.format("%s + %s", types.getKey(), types.getValue()));
    }
    snapshot.createOrValidate();
  }

  @Test
  public void testBasicTypes() {
    assertEquals(6, BasicTypeManager.ALL_TYPES.length);
    assertEquals(
        DoubleType.INSTANCE,
        BasicTypeManager.combine(BigIntType.INSTANCE, DoubleType.INSTANCE, 10).get());
    assertEquals(
        StringType.INSTANCE,
        BasicTypeManager.combine(BigIntType.INSTANCE, TimestampType.INSTANCE, 50).get());
    assertEquals(
        IntervalType.INSTANCE,
        BasicTypeManager.combine(BigIntType.INSTANCE, IntervalType.INSTANCE, 40).get());
  }
}
