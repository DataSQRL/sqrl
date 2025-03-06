/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible.input;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.SpecialName;
import com.datasqrl.io.schema.flexible.constraint.ConstraintHelper;

public class FlexibleSchemaHelper {

  public static boolean isNonNull(FlexibleFieldSchema.Field field) {
    for (FlexibleFieldSchema.FieldType type : field.getTypes()) {
      if (!ConstraintHelper.isNonNull(type.getConstraints())) {
        return false;
      }
    }
    return true;
  }

  public static Name getCombinedName(FlexibleFieldSchema.Field field,
                                     FlexibleFieldSchema.FieldType type) {
    Name name = field.getName();
    if (name instanceof SpecialName) {
      if (name.equals(SpecialName.VALUE)) {
        name = Name.system(
            "_value"); //TODO: Need to check if this clashes with other names in RelationType
      } else {
        throw new IllegalArgumentException(String.format("Unrecognized name: %s", name));
      }
    }

    if (!type.getVariantName().equals(SpecialName.SINGLETON)) {
      //TODO Temporarily skip variant naming
//            name = Name.combine(field.getName(),type.getVariantName());
      name = field.getName();
    }
    return name;
  }

}
