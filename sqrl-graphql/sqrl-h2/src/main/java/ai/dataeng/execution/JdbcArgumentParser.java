package ai.dataeng.execution;

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
import graphql.com.google.common.collect.Maps;
import graphql.schema.DataFetchingEnvironment;
import java.util.Map;
import lombok.Value;

/**
 * Assumes a (filter: ..) structure
 */
public class JdbcArgumentParser extends
    H2ColumnVisitor2<Object, ArgumentContext> {

  @Override
  public Object visitColumns(Columns columns, ArgumentContext context) {
    DataFetchingEnvironment environment = context.getEnvironment();
    Map<String, Object> arguments = environment.getArguments();
    if (arguments.get("filter") == null) {
      return null;
    }
    Map<String, H2Column> columnMap = Maps.uniqueIndex(columns.getColumns(), e->e.getName());
    ColumnVisitor visitor = new ColumnVisitor(context);
    Map<String, Object> filter = (Map<String, Object>) arguments.get("filter");
    for (Map.Entry<String, Object> entry : filter.entrySet()) {
      H2Column column = columnMap.get(entry.getKey());
      column.accept(visitor, entry.getValue());
    }

    return null;
  }

  @Value
  public class ColumnVisitor extends H2ColumnVisitor2<Object, Object> {
    ArgumentContext argumentContext;

    @Override
    public Object visitH2Column(H2Column column, Object context) {
      throw new RuntimeException("");
    }

    @Override
    public Object visitIntegerColumn(IntegerColumn column, Object context) {
      return visitNumberColumn(column, context);
    }

    @Override
    public Object visitBooleanColumn(BooleanColumn column, Object context) {
      return visitEqualityColumn(column, column);
    }

    @Override
    public Object visitStringColumn(StringColumn column, Object context) {
      return visitEqualityColumn(column, column);
    }

    @Override
    public Object visitDateColumn(DateColumn column, Object context) {
      return super.visitDateColumn(column, context);
    }

    @Override
    public Object visitDateTimeColumn(DateTimeColumn column, Object context) {
      return super.visitDateTimeColumn(column, context);
    }

    @Override
    public Object visitFloatColumn(FloatColumn column, Object context) {
      return visitNumberColumn(column, context);
    }

    @Override
    public Object visitScalarArrayColumn(ScalarArrayColumn column, Object context) {
      return super.visitScalarArrayColumn(column, context);
    }

    @Override
    public Object visitTimeColumn(TimeColumn column, Object context) {
      return super.visitTimeColumn(column, context);
    }

    @Override
    public Object visitUUIDColumn(UUIDColumn column, Object context) {
      return visitEqualityColumn(column, context);
    }

    @Override
    public Object visitZonedDateTimeColumn(ZonedDateTimeColumn column, Object context) {
      return super.visitZonedDateTimeColumn(column, context);
    }

    @Override
    public Object visitZonedTimeColumn(ZonedTimeColumn column, Object context) {
      return super.visitZonedTimeColumn(column, context);
    }

    public Object visitNumberColumn(H2Column column, Object context) {
      Map<String, Object> map = (Map<String, Object>)context;
      for (Map.Entry<String, Object> filter : map.entrySet()) {
        String sql;
        switch (filter.getKey()) {
          default:
          case "equals":
            sql = "%s = ?";
            break;
          case "lt":
            sql = "%s < ?";
            break;
          case "lteq":
            sql = "%s <= ?";
            break;
          case "gt":
            sql = "%s > ?";
            break;
          case "gteq":
            sql = "%s >= ?";
            break;
        }
        addArgument(column, sql, filter.getValue());
      }

      return null;
    }

    public Object visitEqualityColumn(H2Column column, Object context) {
      Map<String, Object> map = (Map<String, Object>)context;
      for (Map.Entry<String, Object> filter : map.entrySet()) {
        addArgument(column, "%s = ?", filter.getValue());
      }
      return null;
    }

    private void addArgument(H2Column column, String sql, Object value) {
      argumentContext
          .addArgument(column, String.format(sql, column.getPhysicalName()), value);
    }
  }
}
