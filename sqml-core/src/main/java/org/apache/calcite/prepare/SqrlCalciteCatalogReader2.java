package org.apache.calcite.prepare;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataTypeFactory;

/**
 * Delegates a PreparingTable to SqrlRelOptTable.
 *
 * This is only valid per-query
 */
@Slf4j
public class SqrlCalciteCatalogReader2 extends CalciteCatalogReader {


  public SqrlCalciteCatalogReader2(CalciteSchema rootSchema,
      List<String> defaultSchema,
      RelDataTypeFactory typeFactory,
      CalciteConnectionConfig config) {
    super(rootSchema, defaultSchema, typeFactory, config);
  }
}
