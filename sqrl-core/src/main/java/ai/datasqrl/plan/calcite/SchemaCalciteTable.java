package ai.datasqrl.plan.calcite;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.ScannableTable;

public class SchemaCalciteTable extends SqrlAbstractTable implements ScannableTable {

  private final List<RelDataTypeField> mutableFields;

  public SchemaCalciteTable(List<RelDataTypeField> fieldList) {
    mutableFields = new ArrayList(fieldList);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    FieldInfoBuilder builder = relDataTypeFactory.builder();
    builder.kind(StructKind.FULLY_QUALIFIED);

    mutableFields
        .forEach(builder::add);

    return builder.build();
  }

  public void addField(RelDataTypeField field) {
    mutableFields.add(field);
  }

  /** Returns an enumerable over a given projection of the fields. */
  public Enumerable<Object> project() {
    return new AbstractEnumerable<Object>() {
      @Override
      public Enumerator<Object> enumerator() {
       return Linq4j.asEnumerable(List.of()).enumerator();
      }
    };
  }

  @Override
  public Enumerable<Object[]> scan(DataContext dataContext) {
    return Linq4j.asEnumerable(List.of());
  }
}
