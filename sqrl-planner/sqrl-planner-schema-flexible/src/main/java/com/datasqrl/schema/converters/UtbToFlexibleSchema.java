package com.datasqrl.schema.converters;

import com.datasqrl.canonicalizer.SpecialName;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.constraint.NotNull;
import com.datasqrl.schema.input.FlexibleFieldSchema.Field;
import com.datasqrl.schema.input.FlexibleFieldSchema.FieldType;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.schema.input.FlexibleTableSchemaHolder;
import com.datasqrl.schema.input.RelationType;
import com.datasqrl.schema.input.SchemaElementDescription;
import java.util.ArrayList;
import java.util.List;

public class UtbToFlexibleSchema {

  public static FlexibleTableSchemaHolder createFlexibleSchema(UniversalTable table) {
    FlexibleTableSchema schema = new FlexibleTableSchema(
        table.getName(), new SchemaElementDescription(""), null, false,
        createFields(table),
        List.of());
    return new FlexibleTableSchemaHolder(schema);
  }

  private static RelationType<Field> createFields(UniversalTable table) {
    return createFields(table.getFields().getFields());
  }

  private static RelationType<Field> createFields(List<com.datasqrl.schema.Field> f) {
    List<Field> fields = new ArrayList<>();
    for (com.datasqrl.schema.Field field : f) {
      if (field.getName().isHidden()) { //todo: what if a user defines a _ingest_time in the schema.yml?
        continue;
      }
      if (field instanceof UniversalTable.Column) {
        UniversalTable.Column column = (UniversalTable.Column) field;
        fields.add(new Field(
            field.getName(),
            new SchemaElementDescription(""),
            null,
            List.of(new FieldType(
                SpecialName.SINGLETON,
                UtbTypeToFlexibleType.toType(column), //other types
                0,
                column.isNullable() ? List.of() : List.of(NotNull.INSTANCE) //not null
            ))
        ));
      } else if (field instanceof UniversalTable.ChildRelationship) {
        UniversalTable.ChildRelationship rel = (UniversalTable.ChildRelationship) field;

        RelationType relType = createFields(rel.getChildTable().getFields().getFields());
        fields.add(new Field(
            field.getName(),
            new SchemaElementDescription(""),
            null,
            List.of(new FieldType(
                SpecialName.SINGLETON,
                relType,
                rel.getMultiplicity() == Multiplicity.MANY ? 1 : 0, //todo Array
                List.of()// : List.of(NotNull.INSTANCE)
            ))
        ));
      } else {
        throw new RuntimeException();
      }
    }
    return new RelationType<>(fields);
  }
}
