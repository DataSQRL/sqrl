package com.datasqrl.calcite;

import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;

public class SqrlTableFunction extends SqlUserDefinedTableFunction {
  public SqrlTableFunction(SqlIdentifier opName, SqlKind kind, SqlReturnTypeInference returnTypeInference, SqlOperandTypeInference operandTypeInference, SqlOperandMetadata operandMetadata, TableFunction function) {
    super(opName, kind, returnTypeInference, operandTypeInference, operandMetadata, function);
  }
  // Table functions that will always return a table or other rel node in the schema
}
