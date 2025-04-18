package com.datasqrl.io.schema.flexible.converters;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.SpecialName;
import com.datasqrl.io.schema.flexible.constraint.Constraint;
import com.datasqrl.io.schema.flexible.constraint.NotNull;
import com.datasqrl.io.schema.flexible.input.FlexibleFieldSchema.Field;
import com.datasqrl.io.schema.flexible.input.FlexibleFieldSchema.FieldType;
import com.datasqrl.io.schema.flexible.input.FlexibleTableSchema;
import com.datasqrl.io.schema.flexible.FlexibleTableSchemaHolder;
import com.datasqrl.io.schema.flexible.input.RelationType;
import com.datasqrl.io.schema.flexible.input.SchemaElementDescription;
import com.datasqrl.util.CalciteUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

public class RelDataTypeToFlexibleSchema {

  private static final NameCanonicalizer canonicalizer = NameCanonicalizer.SYSTEM;

  public static FlexibleTableSchemaHolder createFlexibleSchema(RelDataTypeField tableType) {
    FlexibleTableSchema schema = new FlexibleTableSchema(
        Name.system(tableType.getName()), new SchemaElementDescription(""), null, false,
        createFields(tableType.getType().getFieldList()),
        List.of());
    return new FlexibleTableSchemaHolder(schema);
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
                RelDataTypePrimitiveToFlexibleType.toType(fieldType), //other types
                arrayDepth,
                constraints
            ))
        ));
      }
    }
    return new RelationType<>(fields);
  }



}
