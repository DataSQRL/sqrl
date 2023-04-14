/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.schema.converters.RowConstructor;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.Arrays;

public class FlinkRowConstructor implements RowConstructor<Row> {

  public static final FlinkRowConstructor INSTANCE = new FlinkRowConstructor();

  @Override
  public Row createRow(Object[] columns) {
    return Row.ofKind(RowKind.INSERT, columns);
  }

  @Override
  public Row createNestedRow(Object[] columns) {
    return Row.of(columns);
  }

  @Override
  public Row[] createRowList(Object[] rows) {
    return Arrays.copyOf(rows, rows.length, Row[].class);
  }
}
