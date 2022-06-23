package ai.datasqrl.plan.calcite;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

@AllArgsConstructor
public class DataTable extends AbstractTable implements QueryableTable {
  private final List<RelDataTypeField> header;
  private final Collection<Object[]> elements;

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    FieldInfoBuilder builder = relDataTypeFactory.builder();
    builder.kind(StructKind.FULLY_QUALIFIED);

    header
        .forEach(builder::add);

    return builder.build();
  }

  /** Returns an enumerable over a given projection of the fields. */
  @SuppressWarnings("unused")
  public Enumerable<Object> project(DataContext context) {
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object> enumerator() {
       return toEnumerator(elements);
      }
    };
  }

  public Type getElementType() {
    return Object[].class;
  }

  public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
    return Schemas.tableExpression(schema, Object[].class, tableName, clazz);
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
      String tableName) {
    return new AbstractTableQueryable<>(queryProvider, schema, this,
        tableName) {
      @Override public Enumerator<T> enumerator() {
        return toEnumerator(elements);
      }
    };
  }

  private <T> Enumerator<T> toEnumerator(Collection<Object[]> elements) {
    Object[] list = elements.toArray(new Object[0]);

    for (int i = 0; i < elements.size(); i++) {
      Object[] o = (Object[])list[i];
      for (int j = 0; j <o.length; j++) {
        if (o[j] instanceof Object[]) {
          o[j] = boxAsList((Object[]) o[j]);
        }
      }

    }
    return (Enumerator<T>) Linq4j.asEnumerable(list).enumerator();
  }

  private List boxAsList(Object[] elements) {
    List list = new ArrayList();
    for (Object o : elements) {
      if (o instanceof Object[]) {
        list.add(boxAsList((Object[]) o));
      } else {
        list.add(o);
      }
    }
    return list;
  }
}