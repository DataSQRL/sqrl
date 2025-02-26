package org.apache.calcite.sql;

public interface ScriptVisitor<R, C> {
  R visit(ScriptNode statement, C context);
}
