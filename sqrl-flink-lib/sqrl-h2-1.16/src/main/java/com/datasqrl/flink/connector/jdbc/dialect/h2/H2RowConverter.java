/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink.connector.jdbc.dialect.h2;

import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

public class H2RowConverter extends AbstractJdbcRowConverter {

  public H2RowConverter(RowType rowType) {
    super(rowType);
  }

  @Override
  public String converterName() {
    return "H2";
  }
}
