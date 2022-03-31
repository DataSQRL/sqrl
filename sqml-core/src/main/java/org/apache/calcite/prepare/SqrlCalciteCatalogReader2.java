package org.apache.calcite.prepare;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CachingSqrlSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.Prepare.PreparingTable;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.validate.ListScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

/**
 * Delegates a PreparingTable to SqrlRelOptTable.
 *
 * This is only valid per-query
 */
@Slf4j
public class SqrlCalciteCatalogReader2 extends CalciteCatalogReader {

  private final CalciteSchema rootSqrlSchema;

  public SqrlCalciteCatalogReader2(CalciteSchema rootSchema,
      List<String> defaultSchema,
      RelDataTypeFactory typeFactory,
      CalciteConnectionConfig config) {
    super(rootSchema, defaultSchema, typeFactory, config);
    this.rootSqrlSchema = rootSchema;
  }
}
