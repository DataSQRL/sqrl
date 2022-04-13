package ai.dataeng.execution;

import ai.dataeng.execution.RowMapperBuilder.ResultContext;
import ai.dataeng.execution.page.PageProvider;
import ai.dataeng.execution.table.H2ColumnVisitor2;
import ai.dataeng.execution.table.column.BooleanColumn;
import ai.dataeng.execution.table.column.Columns;
import ai.dataeng.execution.table.column.DateColumn;
import ai.dataeng.execution.table.column.DateTimeColumn;
import ai.dataeng.execution.table.column.FloatColumn;
import ai.dataeng.execution.table.column.H2Column;
import ai.dataeng.execution.table.column.IntegerColumn;
import ai.dataeng.execution.table.column.ScalarArrayColumn;
import ai.dataeng.execution.table.column.StringColumn;
import ai.dataeng.execution.table.column.TimeColumn;
import ai.dataeng.execution.table.column.UUIDColumn;
import ai.dataeng.execution.table.column.ZonedDateTimeColumn;
import ai.dataeng.execution.table.column.ZonedTimeColumn;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.Value;

@Value
//Todo: list & point queries are row mapping behavior
public class RowMapperBuilder extends H2ColumnVisitor2<Object, ResultContext> {
  PageProvider pageProvider;
  @Override
  public Object visitColumns(Columns columns, ResultContext context) {
    //Todo: Selected columns?

    RowMapperBuilder visitor = this;
    return new Function<RowSet<Row>, Object>() {
      //Good codegen candidate
      @Override
      public Object apply(RowSet<Row> rows) {
        List results = new ArrayList();
        for (Row row : rows) {
          ResultContext resultContext = new ResultContext(new HashMap(), row);
          for (H2Column column : columns.getColumns()) {
            column.accept(visitor, resultContext);
          }

          results.add(resultContext.getMap());
        }

        return pageProvider.wrap(results, "test", true);
      }
    };
  }

  @Override
  public Object visitH2Column(H2Column column, ResultContext context) {
    return super.visitH2Column(column, context);
  }

  @Override
  public Object visitIntegerColumn(IntegerColumn column, ResultContext context) {
    context.getMap().put(column.getName(),
        context.getRow().getInteger(column.getPhysicalName()));
    return null;
  }

  @Override
  public Object visitBooleanColumn(BooleanColumn column, ResultContext context) {
    context.getMap().put(column.getName(),
        context.getRow().getBoolean(column.getPhysicalName()));
    return null;
  }

  @Override
  public Object visitDateColumn(DateColumn column, ResultContext context) {
    context.getMap().put(column.getName(),
        context.getRow().getLocalDate(column.getPhysicalName()));
    return null;
  }

  @Override
  public Object visitDateTimeColumn(DateTimeColumn column, ResultContext context) {
    context.getMap().put(column.getName(),
        context.getRow().getLocalDateTime(column.getPhysicalName()));
    return null;
  }

  @Override
  public Object visitFloatColumn(FloatColumn column, ResultContext context) {
    context.getMap().put(column.getName(),
        context.getRow().getFloat(column.getPhysicalName()));
    return null;
  }

  @Override
  public Object visitScalarArrayColumn(ScalarArrayColumn column, ResultContext context) {
    return super.visitScalarArrayColumn(column, context);
  }

  @Override
  public Object visitTimeColumn(TimeColumn column, ResultContext context) {
    context.getMap().put(column.getName(),
        context.getRow().getLocalTime(column.getPhysicalName()));
    return null;
  }

  @Override
  public Object visitUUIDColumn(UUIDColumn column, ResultContext context) {
    context.getMap().put(column.getName(),
        context.getRow().getString(column.getPhysicalName()));
    return null;
  }

  @Override
  public Object visitZonedDateTimeColumn(ZonedDateTimeColumn column, ResultContext context) {
    context.getMap().put(column.getName(),
        context.getRow().getOffsetDateTime(column.getPhysicalName()));
    return null;
  }

  @Override
  public Object visitZonedTimeColumn(ZonedTimeColumn column, ResultContext context) {
    context.getMap().put(column.getName(),
        context.getRow().getOffsetTime(column.getPhysicalName()));
    return null;
  }

  @Override
  public Object visitStringColumn(StringColumn column, ResultContext context) {
    return  context.getMap().put(column.getName(),
        context.getRow().getString(column.getPhysicalName()));
  }

  @Value
  public class ResultContext {
    Map map;
    Row row;
  }
}
