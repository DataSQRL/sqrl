package ai.datasqrl.graphql.execution.table;

import ai.datasqrl.graphql.execution.table.column.BooleanColumn;
import ai.datasqrl.graphql.execution.table.column.Columns;
import ai.datasqrl.graphql.execution.table.column.DateColumn;
import ai.datasqrl.graphql.execution.table.column.DateTimeColumn;
import ai.datasqrl.graphql.execution.table.column.FloatColumn;
import ai.datasqrl.graphql.execution.table.column.H2Column;
import ai.datasqrl.graphql.execution.table.column.IntegerColumn;
import ai.datasqrl.graphql.execution.table.column.PrimaryKeyColumn;
import ai.datasqrl.graphql.execution.table.column.ScalarArrayColumn;
import ai.datasqrl.graphql.execution.table.column.StringColumn;
import ai.datasqrl.graphql.execution.table.column.TimeColumn;
import ai.datasqrl.graphql.execution.table.column.UUIDColumn;
import ai.datasqrl.graphql.execution.table.column.ZonedDateTimeColumn;
import ai.datasqrl.graphql.execution.table.column.ZonedTimeColumn;

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
