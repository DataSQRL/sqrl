/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.discovery.preprocessor;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.SpecialName;
import com.datasqrl.canonicalizer.StandardName;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.constraint.Constraint;
import com.datasqrl.io.schema.flexible.input.FlexibleFieldSchema;
import com.datasqrl.io.schema.flexible.input.FlexibleFieldSchema.Field;
import com.datasqrl.io.schema.flexible.input.FlexibleFieldSchema.FieldType;
import com.datasqrl.io.schema.flexible.input.RelationType;
import com.datasqrl.io.schema.flexible.type.Type;
import com.datasqrl.io.schema.flexible.type.basic.BasicType;
import com.datasqrl.io.schema.flexible.type.basic.BasicTypeManager;
import java.util.List;
import java.util.Map;

record FlexibleSchemaJson(List<FieldJson> fields) {

  private static final List<Name> SPECIAL_NAMES =
      List.of(SpecialName.SINGLETON, SpecialName.LOCAL, SpecialName.VALUE);

  record NameJson(String canonical, String display) {

    static NameJson of(Name name) {
      return new NameJson(name.getCanonical(), name.getDisplay());
    }

    Name toName() {
      return SPECIAL_NAMES.stream()
          .filter(special -> special.getCanonical().equals(canonical))
          .findFirst()
          .orElseGet(() -> new StandardName(canonical, display));
    }
  }

  record ConstraintJson(String name, Map<String, Object> parameters) {

    static ConstraintJson of(Constraint constraint) {
      return new ConstraintJson(constraint.getName().getDisplay(), constraint.export());
    }

    Constraint toConstraint() {
      var factory = Constraint.FACTORY_LOOKUP.get(name);
      if (factory == null) {
        throw new IllegalArgumentException("Unknown constraint: " + name);
      }
      var errors = ErrorCollector.root();
      var constraint = factory.create(parameters == null ? Map.of() : parameters, errors);
      if (constraint.isEmpty()) {
        throw new IllegalArgumentException("Invalid constraint %s: %s".formatted(name, errors));
      }
      return constraint.get();
    }
  }

  record FieldTypeJson(
      NameJson variant,
      String basicType,
      List<FieldJson> fields,
      int arrayDepth,
      List<ConstraintJson> constraints) {

    static FieldTypeJson of(FieldType fieldType) {
      var type = fieldType.getType();
      String basicType = null;
      List<FieldJson> fields = null;
      if (type instanceof BasicType<?> basic) {
        basicType = basic.getName();
      } else if (type instanceof RelationType<?> relation) {
        fields = fieldsOf((RelationType<Field>) relation);
      } else {
        throw new IllegalArgumentException("Unsupported field type: " + type);
      }
      return new FieldTypeJson(
          NameJson.of(fieldType.getVariantName()),
          basicType,
          fields,
          fieldType.getArrayDepth(),
          fieldType.getConstraints().stream().map(ConstraintJson::of).toList());
    }

    FieldType toFieldType() {
      Type type;
      if (basicType != null) {
        type = BasicTypeManager.getTypeByName(basicType);
        if (type == null) {
          throw new IllegalArgumentException("Unknown type: " + basicType);
        }
      } else {
        type = toRelation(fields);
      }
      return new FieldType(
          variant.toName(),
          type,
          arrayDepth,
          constraints.stream().map(ConstraintJson::toConstraint).toList());
    }
  }

  record FieldJson(NameJson name, List<FieldTypeJson> types) {

    static FieldJson of(Field field) {
      return new FieldJson(
          NameJson.of(field.getName()), field.getTypes().stream().map(FieldTypeJson::of).toList());
    }

    Field toField() {
      var builder = new FlexibleFieldSchema.Field.Builder();
      builder.setName(name.toName());
      builder.setTypes(types.stream().map(FieldTypeJson::toFieldType).toList());
      return builder.build();
    }
  }

  static FlexibleSchemaJson of(RelationType<Field> relation) {
    return new FlexibleSchemaJson(fieldsOf(relation));
  }

  private static List<FieldJson> fieldsOf(RelationType<Field> relation) {
    return relation.getFields().stream().map(FieldJson::of).toList();
  }

  RelationType<Field> toRelation() {
    return toRelation(fields);
  }

  private static RelationType<Field> toRelation(List<FieldJson> fields) {
    RelationType.Builder<Field> builder = RelationType.build();
    fields.forEach(field -> builder.add(field.toField()));
    return builder.build();
  }
}
