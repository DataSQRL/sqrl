package com.datasqrl.calcite;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.List;

public class CatalogReader extends CalciteCatalogReader {
  public CatalogReader(CalciteSchema rootSchema, RelDataTypeFactory typeFactory, CalciteConnectionConfig config) {
    super(rootSchema, List.of(), typeFactory, config);
  }
}
