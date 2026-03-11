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
package com.datasqrl.calcite.type;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeUtil;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TypeCompatibility {

  /** Checks if newType can read data produced by oldType. */
  public static boolean isBackwardsCompatible(RelDataType newType, RelDataType oldType) {
    // 1. Nullability Check
    // If the writer (b) could produce nulls, the reader (a) MUST be able to handle them.
    if (oldType.isNullable() && !newType.isNullable()) {
      return false;
    }

    // 2. Structural Recursion: Row/Struct Types
    if (newType.isStruct() && oldType.isStruct()) {
      return checkStructCompatibility(newType, oldType);
    }

    // 3. Structural Recursion: Array/Collection Types
    if (newType.getComponentType() != null && oldType.getComponentType() != null) {
      return isBackwardsCompatible(newType.getComponentType(), oldType.getComponentType());
    }

    // 4. Structural Recursion: Map Types
    if (newType.getKeyType() != null && oldType.getKeyType() != null) {
      return isBackwardsCompatible(newType.getKeyType(), oldType.getKeyType())
          && isBackwardsCompatible(newType.getValueType(), oldType.getValueType());
    }

    // 5. Leaf Types (Primitives, Decimals, etc.)
    // Use Calcite's assignment compatibility for basic type family checks
    if (!SqlTypeUtil.canAssignFrom(newType, oldType)) {
      return false;
    }

    // Reject narrowing: newType must be able to hold all values from oldType.
    // For numeric/string/temporal types, this means newType's precision must be >= oldType's.
    return newType.getPrecision() >= oldType.getPrecision();
  }

  private static boolean checkStructCompatibility(RelDataType newType, RelDataType oldType) {
    // In the Avro sense, the reader (new) doesn't need to contain all fields from the writer (old).
    // However, any field the reader expects that is NOT in the writer must be nullable.

    for (RelDataTypeField readerField : newType.getFieldList()) {
      // Find the corresponding field in the writer schema by name
      RelDataTypeField writerField = oldType.getField(readerField.getName(), false, false);

      if (writerField != null) {
        // Field exists in both: Recurse to check type widening/compatibility
        if (!isBackwardsCompatible(readerField.getType(), writerField.getType())) {
          return false;
        }
      } else {
        // Field is NEW: It must be nullable to be compatible with old data
        if (!readerField.getType().isNullable()) {
          return false;
        }
      }
    }

    // Note: Fields in 'writer' that aren't in 'reader' are ignored (compatible).
    return true;
  }
}
