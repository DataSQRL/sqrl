/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import com.datasqrl.InputError;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import java.util.List;
import org.junit.jupiter.api.Test;

public class FlinkSerialization {

  @Test
  public void testPojoSerialization() {
    List<Class> streamDataTypes = List.of(TimeAnnotatedRecord.class,
        InputError.class, SourceRecord.Raw.class, SourceRecord.Named.class);
    for (Class clazz : streamDataTypes) {
      //Use PojoTestUtils once we are at Flink version 1.17 to verify
      //that the classes we use in the DataStream are valid Pojos
    }
  }

}
