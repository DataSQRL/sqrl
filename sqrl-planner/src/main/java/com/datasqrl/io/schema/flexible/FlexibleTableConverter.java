/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.schema.flexible.constraint.Cardinality;
import com.datasqrl.io.schema.flexible.constraint.ConstraintHelper;
import com.datasqrl.io.schema.flexible.input.FlexibleFieldSchema;
import com.datasqrl.io.schema.flexible.input.FlexibleSchemaHelper;
import com.datasqrl.io.schema.flexible.input.FlexibleTableSchema;
import com.datasqrl.io.schema.flexible.input.RelationType;
import com.datasqrl.io.schema.flexible.type.ArrayType;
import com.datasqrl.io.schema.flexible.type.Type;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class FlexibleTableConverter {

  private final FlexibleTableSchema schema;
  private final Name tableName;

  public Name getName() {
    return tableName;
  }

  public <T> T apply(Visitor<T> visitor) {
    return visitRelation(NamePath.ROOT, getName(),
        schema.getFields(),
        false, true, visitor);
  }

  private <T> T visitRelation(NamePath path, Name name,
      RelationType<FlexibleFieldSchema.Field> relation,
      boolean isNested, boolean isSingleton, Visitor<T> visitor) {
    visitor.beginTable(name, path, isNested, isSingleton);
    path = path.concat(name);

    for (FlexibleFieldSchema.Field field : relation.getFields()) {
      for (FlexibleFieldSchema.FieldType ftype : field.getTypes()) {
        Name fieldName = FlexibleSchemaHelper.getCombinedName(field, ftype);
        boolean isMixedType = field.getTypes().size() > 1;
        visitFieldType(path, fieldName, ftype, isMixedType, visitor);
      }
    }
    return visitor.endTable(name, path, isNested, isSingleton);
  }

  private <T> void visitFieldType(NamePath path, Name fieldName,
      FlexibleFieldSchema.FieldType ftype,
      boolean isMixedType, Visitor<T> visitor) {
    boolean nullable = isMixedType || !ConstraintHelper.isNonNull(ftype.getConstraints());
    boolean isSingleton = false;
    if (ftype.getType() instanceof RelationType) {
      isSingleton = isSingleton(ftype);
      T nestedTable = visitRelation(path, fieldName,
          (RelationType<FlexibleFieldSchema.Field>) ftype.getType(), true,
          isSingleton, visitor);
      nullable = isMixedType || hasZeroOneMultiplicity(ftype);
      if (ConstraintHelper.isNonNull(ftype.getConstraints())) {
        nullable = false;
      }
      visitor.addField(fieldName, nestedTable, nullable, isSingleton);
    } else {
      //Make array if it has arrayDepth
      Type type = ftype.getType();
      for (int i = 0; i < ftype.getArrayDepth(); i++) {
        type = new ArrayType(type);
      }
      visitor.addField(fieldName, type, nullable);
    }
  }

  private static boolean isSingleton(FlexibleFieldSchema.FieldType ftype) {
    return ConstraintHelper.getCardinality(ftype.getConstraints()).isSingleton();
  }

  private static boolean hasZeroOneMultiplicity(FlexibleFieldSchema.FieldType ftype) {
    Cardinality card = ConstraintHelper.getCardinality(ftype.getConstraints());
    return card.isSingleton() && card.getMin() == 0;
  }

  public interface Visitor<T> {

    void beginTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton);

    T endTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton);

    void addField(Name name, Type type, boolean nullable);

    void addField(Name name, T nestedTable, boolean nullable, boolean isSingleTon);
  }

}
