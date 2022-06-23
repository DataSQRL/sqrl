package ai.datasqrl.plan.calcite.sqrl.table;

import java.lang.reflect.Type;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.impl.AbstractTable;

public abstract class AbstractSqrlTable extends AbstractTable implements QueryableTable {

  public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
    return Schemas.tableExpression(schema, Object[].class, tableName, clazz);
  }


  //This is only here so calcite can set up the correct model
  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
      String tableName) {
    throw new RuntimeException("");
  }

  @Override
  public Type getElementType() {
    return Object[].class;
  }
}
