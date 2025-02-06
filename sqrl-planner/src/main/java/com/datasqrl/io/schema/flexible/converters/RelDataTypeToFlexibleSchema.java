package com.datasqrl.io.schema.flexible.converters;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.SpecialName;
import com.datasqrl.io.schema.flexible.FlexibleTableSchemaHolder;
import com.datasqrl.schema.constraint.NotNull;
import com.datasqrl.schema.input.FlexibleFieldSchema.Field;
import com.datasqrl.schema.input.FlexibleFieldSchema.FieldType;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.schema.input.RelationType;
import com.datasqrl.schema.input.SchemaElementDescription;
import com.datasqrl.util.CalciteUtil;

public class RelDataTypeToFlexibleSchema {

  private static final NameCanonicalizer canonicalizer = NameCanonicalizer.SYSTEM;

  public static FlexibleTableSchemaHolder createFlexibleSchema(RelDataTypeField tableType) {
    var schema = new FlexibleTableSchema(
        Name.system(tableType.getName()), new SchemaElementDescription(""), null, false,
        createFields(tableType.getType().getFieldList()),
        List.of());
    return new FlexibleTableSchemaHolder(schema);
  }

  private static RelationType<Field> createFields(List<RelDataTypeField> relFields) {
    List<Field> fields = new ArrayList<>();
    for (RelDataTypeField field: relFields) {
      var constraints = field.getType().isNullable()?List.of():List.of(NotNull.INSTANCE);
      var nestedType = CalciteUtil.getNestedTableType(field.getType());
      var fieldName = canonicalizer.name(field.getName());
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
        var fieldType = field.getType();
        var arrayDepth = 0;
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
