package ai.datasqrl.plan.calcite;

import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.schema.Table;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

public class SourceCalciteTable extends AbstractQueryableTable {

  private final RelDataType rowType;
  Table table;
  SourceTable source;
  SourceDataEnumerator enumerator;

  public SourceCalciteTable(RelDataType rowType){
//      Table table, SourceTableImport sourceTableImport) {
    super(Object[].class);
//    rowType = table.getHead().getRowType();
    this.rowType = rowType;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return rowType;
  }

  public Enumerable<Object> project(final DataContext root) {
    return new AbstractEnumerable<>() {
      @Override public Enumerator<Object> enumerator() {
        return Linq4j.asEnumerable(
            List.of(new Object[]{new Object[]{"3571", "x"}})
            )
            .enumerator();
      }
    };
  }
  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
      String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this,
        tableName) {
//      @SuppressWarnings("unchecked")
      @Override public Enumerator<T> enumerator() {
        return (Enumerator<T>)Linq4j.asEnumerable(
                List.of(
                    new Object[]{
                        new Object[]{3571L, "Poptech Blow 500", "High powered blowdryer for any hair", "Personal Care", 0, 0, 0}})
            ).enumerator();
      }
    };
  }
}
