package org.apache.calcite.jdbc;

public interface CalciteSchemaVisitor<R, C> {

  R visit(SqrlSchema schema, C context);
}
