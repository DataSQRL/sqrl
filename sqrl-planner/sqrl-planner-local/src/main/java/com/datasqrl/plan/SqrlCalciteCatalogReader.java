/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan;

import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.List;

public class SqrlCalciteCatalogReader extends CalciteCatalogReader {

  @Getter
  private final SqrlSchema sqrlRootSchema;

  public SqrlCalciteCatalogReader(SqrlSchema rootSchema,
      List<String> defaultSchema,
      RelDataTypeFactory typeFactory,
      CalciteConnectionConfig config) {
    super(rootSchema, defaultSchema, typeFactory, config);
    this.sqrlRootSchema = rootSchema;
  }
}
