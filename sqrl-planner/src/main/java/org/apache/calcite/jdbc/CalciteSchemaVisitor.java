package org.apache.calcite.jdbc;

public interface CalciteSchemaVisitor<R, C> {

  R visit(SqrlCalciteSchema schema, C context);
}
