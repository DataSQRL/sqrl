package ai.dataeng.execution.table;

import ai.dataeng.execution.table.column.BooleanColumn;
import ai.dataeng.execution.table.column.Column;
import ai.dataeng.execution.table.column.Columns;
import ai.dataeng.execution.table.column.DateColumn;
import ai.dataeng.execution.table.column.DateTimeColumn;
import ai.dataeng.execution.table.column.FloatColumn;
import ai.dataeng.execution.table.column.H2Column;
import ai.dataeng.execution.table.column.IntegerColumn;
import ai.dataeng.execution.table.column.PrimaryKeyColumn;
import ai.dataeng.execution.table.column.ScalarArrayColumn;
import ai.dataeng.execution.table.column.StringColumn;
import ai.dataeng.execution.table.column.TimeColumn;
import ai.dataeng.execution.table.column.UUIDColumn;
import ai.dataeng.execution.table.column.ZonedDateTimeColumn;
import ai.dataeng.execution.table.column.ZonedTimeColumn;

public class H2ColumnVisitor2<R, C> {

  public R visitColumns(Columns columns, C context) {
    return null;
  }

  public R visitH2Column(H2Column column, C context) {
    return null;
  }

  public R visitIntegerColumn(IntegerColumn column, C context) {
    return visitH2Column(column, context);
  }

  public R visitBooleanColumn(BooleanColumn column, C context) {
    return visitH2Column(column, context);
  }

  public R visitDateColumn(DateColumn column, C context) {
    return visitH2Column(column, context);
  }

  public R visitDateTimeColumn(DateTimeColumn column, C context) {
    return visitH2Column(column, context);
  }

  public R visitFloatColumn(FloatColumn column, C context) {
    return visitH2Column(column, context);
  }

  public R visitScalarArrayColumn(ScalarArrayColumn column,
      C context) {
    return visitH2Column(column, context);
  }

  public R visitTimeColumn(TimeColumn column, C context) {
    return visitH2Column(column, context);
  }

  public R visitUUIDColumn(UUIDColumn column, C context) {
    return visitH2Column(column, context);
  }

  public R visitZonedDateTimeColumn(ZonedDateTimeColumn column,
      C context) {
    return visitH2Column(column, context);
  }

  public R visitZonedTimeColumn(ZonedTimeColumn column, C context) {
    return visitH2Column(column, context);
  }
  public R visitPrimaryKeyColumn(PrimaryKeyColumn column, C context) {
    return visitH2Column(column, context);
  }

  public R visitStringColumn(StringColumn column, C context) {
    return visitH2Column(column, context);
  }
}
