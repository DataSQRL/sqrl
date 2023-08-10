package com.datasqrl.calcite;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

public class SqrlSqlValidator extends SqlValidatorImpl {
  /**
   * Creates a validator.
   *
   * @param opTab         Operator table
   * @param catalogReader Catalog reader
   * @param typeFactory   Type factory
   * @param config        Config
   */
  protected SqrlSqlValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory, Config config) {
    super(opTab, catalogReader, typeFactory, config);
  }
  // Your custom implementation here
}
