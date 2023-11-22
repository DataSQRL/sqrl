package com.datasqrl.schema.converters;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.SpecialName;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.constraint.NotNull;
import com.datasqrl.schema.input.FlexibleFieldSchema.Field;
import com.datasqrl.schema.input.FlexibleFieldSchema.FieldType;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.schema.input.FlexibleTableSchemaHolder;
import com.datasqrl.schema.input.RelationType;
import com.datasqrl.schema.input.SchemaElementDescription;
import com.datasqrl.util.CalciteUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

public class UtbToFlexibleSchema {

  private static final NameCanonicalizer canonicalizer = NameCanonicalizer.SYSTEM;

  public static FlexibleTableSchemaHolder createFlexibleSchema(UniversalTable table) {
    FlexibleTableSchema schema = new FlexibleTableSchema(
        table.getName(), new SchemaElementDescription(""), null, false,
        createFields(table),
        List.of());
    return new FlexibleTableSchemaHolder(schema);
  }

  private static RelationType<Field> createFields(UniversalTable table) {
    return createFields(table.getType().getFieldList());
  }

  private static RelationType<Field> createFields(List<RelDataTypeField> relFields) {
    List<Field> fields = new ArrayList<>();
    for (RelDataTypeField field: relFields) {
      List<Constraint> constraints = field.getType().isNullable()?List.of():List.of(NotNull.INSTANCE);
      Optional<RelDataType> nestedType = CalciteUtil.getNestedTableType(field.getType());
      Name fieldName = canonicalizer.name(field.getName());
      if (nestedType.isPresent()) {
        RelationType<?> relType = createFields(nestedType.get().getFieldList());
        fields.add(new Field(
            fieldName,
            new SchemaElementDescription(""),
            null,
            List.of(new FieldType(
                SpecialName.SINGLETON,
                relType,
                CalciteUtil.isArray(field.getType()) ? 1 : 0,
                constraints
            ))
        ));
      } else {
        RelDataType fieldType = field.getType();
        int arrayDepth = 0;
        Optional<RelDataType> elementType;
        while ((elementType = CalciteUtil.getArrayElementType(fieldType)).isPresent()) {
          fieldType = elementType.get();
          arrayDepth++;
        }
        fields.add(new Field(
            fieldName,
            new SchemaElementDescription(""),
            null,
            List.of(new FieldType(
                SpecialName.SINGLETON,
                RelDataTypeToFlexibleType.toType(fieldType), //other types
                arrayDepth,
                constraints
            ))
        ));
      }
    }
    return new RelationType<>(fields);
  }



}
