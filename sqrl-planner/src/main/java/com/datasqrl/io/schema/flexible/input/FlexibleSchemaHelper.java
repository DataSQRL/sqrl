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

  public static Name getCombinedName(
      FlexibleFieldSchema.Field field, FlexibleFieldSchema.FieldType type) {
    Name name = field.getName();
    if (name instanceof SpecialName) {
      if (name.equals(SpecialName.VALUE)) {
        name =
            Name.system(
                "_value"); // TODO: Need to check if this clashes with other names in RelationType
      } else {
        throw new IllegalArgumentException("Unrecognized name: %s".formatted(name));
      }
    }

    if (!type.getVariantName().equals(SpecialName.SINGLETON)) {
      // TODO Temporarily skip variant naming
      //            name = Name.combine(field.getName(),type.getVariantName());
      name = field.getName();
    }
    return name;
  }
}
