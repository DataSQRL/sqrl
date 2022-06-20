package ai.datasqrl.plan.calcite;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.input.FlexibleTableConverter.Visitor;
import ai.datasqrl.schema.type.basic.BasicType;
import ai.datasqrl.schema.type.basic.DateTimeType;
import ai.datasqrl.schema.type.basic.IntegerType;
import ai.datasqrl.schema.type.basic.UuidType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.commons.lang3.tuple.Pair;

public class ImportTableRelDataTypeFactory implements Visitor<RelDataType> {

  private final JavaTypeFactory typeFactory;
  private final SqrlType2Calcite typeConverter;

  Stack<FieldInfoBuilder> fieldBuilders = new Stack<>();
  Stack<Table> tableStack = new Stack<>();
  private List<Pair<Table, RelDataType>> result = new ArrayList<>();

  public ImportTableRelDataTypeFactory(JavaTypeFactory typeFactory, SqrlType2Calcite typeConverter,
      Table table) {
    this.typeFactory = typeFactory;
    this.typeConverter = typeConverter;
    tableStack.push(table);
    fieldBuilders.push(typeFactory
        .builder().kind(StructKind.FULLY_QUALIFIED));
  }

  @Override
  public void beginTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton, boolean hasSourceTimestamp) {
    Table cur = tableStack.peek();
    FieldInfoBuilder curBuilder = fieldBuilders.peek();
    if (isNested) {
      Relationship rel = (Relationship)cur.walkField(name.toNamePath())
          .get();
      Table table = rel.getToTable();

      tableStack.push(table);
      cur = table;

      FieldInfoBuilder fieldBuilder = typeFactory
          .builder().kind(StructKind.FULLY_QUALIFIED);
      fieldBuilders.push(fieldBuilder);
    }

    for (Field field : cur.getFields().getElements()) {
      if (field instanceof Relationship) continue;
      Column column = (Column) field;
      if (column.getName() == ReservedName.ARRAY_IDX) {
        fieldBuilders.peek().add(column.getName().getCanonical(), convertBasicType(IntegerType.INSTANCE));
      } else if (column.getName() == ReservedName.UUID) {
        fieldBuilders.peek().add(column.getName().getCanonical(), convertBasicType(UuidType.INSTANCE));
      } else if (column.getName() == ReservedName.INGEST_TIME) {
        fieldBuilders.peek().add(column.getName().getCanonical(), convertBasicType(DateTimeType.INSTANCE));
      } else if (column.getName() == ReservedName.SOURCE_TIME) {
        fieldBuilders.peek().add(column.getName().getCanonical(), convertBasicType(DateTimeType.INSTANCE));
      } else if (column.isPrimaryKey() ||
          column.isParentPrimaryKey()) {
        fieldBuilders.peek().add(column.getName().getCanonical(),
            getFieldType(column.getName().getCanonical(), curBuilder));
      }
    }
  }

  private RelDataType getFieldType(String canonical, FieldInfoBuilder peek) {
    for (RelDataTypeField field : peek.build().getFieldList()) {
      if (field.getName().equals(canonical)) {
        return field.getType();
      }
    }
    throw new RuntimeException("Could not find field");
  }

  @Override
  public Optional<RelDataType> endTable(Name name, NamePath namePath, boolean isNested,
      boolean isSingleton) {
    FieldInfoBuilder completed = fieldBuilders.pop();
    Table table = tableStack.pop();

    RelDataType fields = completed.build();
    result.add(Pair.of(table, fields));
    return Optional.of(fields);
  }

  @Override
  public void addField(Name name, RelDataType type, boolean notnull) {
    if (type instanceof ArraySqlType && type.getComponentType() instanceof RelRecordType) {
      return; //do not add shredded types
    }

    fieldBuilders.peek().add(name.getCanonical(), type).nullable(notnull);
  }

  @Override
  public RelDataType convertBasicType(BasicType type) {
    return type.accept(typeConverter, null);
  }

  @Override
  public RelDataType wrapArray(RelDataType type, boolean notnull) {
    return new ArraySqlType(type, notnull);
  }

  public List<Pair<Table, RelDataType>> getResult() {
    return result;
  }
}
