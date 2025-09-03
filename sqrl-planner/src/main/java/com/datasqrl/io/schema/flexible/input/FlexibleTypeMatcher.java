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

import com.datasqrl.io.schema.flexible.input.FlexibleFieldSchema.FieldType;
import com.datasqrl.io.schema.flexible.type.Type;
import com.datasqrl.io.schema.flexible.type.basic.BasicType;
import com.datasqrl.io.schema.flexible.type.basic.BasicTypeManager;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;

public class FlexibleTypeMatcher implements Serializable {

  protected final SchemaAdjustmentSettings settings;

  public FlexibleTypeMatcher(SchemaAdjustmentSettings settings) {
    this.settings = settings;
  }

  public FlexibleFieldSchema.FieldType matchType(
      TypeSignature typeSignature, List<FieldType> fieldTypes) {
    FlexibleFieldSchema.FieldType match;
    // First, try to match raw type
    match = matchSingleType(typeSignature.raw(), typeSignature.arrayDepth(), fieldTypes, false);
    if (match == null) {
      // Second, try to match on detected
      match =
          matchSingleType(typeSignature.detected(), typeSignature.arrayDepth(), fieldTypes, false);
      if (match == null) {
        // If neither of those worked, try to force a match which means casting raw to STRING if
        // available
        match = matchSingleType(typeSignature.raw(), typeSignature.arrayDepth(), fieldTypes, true);
      }
    }
    return match;
  }

  private FlexibleFieldSchema.FieldType matchSingleType(
      Type type, int arrayDepth, List<FlexibleFieldSchema.FieldType> fieldTypes, boolean force) {
    FlexibleFieldSchema.FieldType match;
    if (type instanceof RelationType) {
      match =
          fieldTypes.stream()
              .filter(ft -> ft.getType() instanceof RelationType)
              .findFirst()
              .orElse(null);
      if (match == null && force) {
        // TODO: Should we consider coercing a relation to string (as json)?
      }
    } else {
      var btype = (BasicType) type;
      List<Pair<Integer, FieldType>> potentialMatches = new ArrayList<>(fieldTypes.size());
      for (FlexibleFieldSchema.FieldType ft : fieldTypes) {
        BasicType basicFieldType;
        int distanceAdjustment;
        if (ft.getType() instanceof BasicType) {
          basicFieldType = (BasicType) ft.getType();
          distanceAdjustment = 1;
        } else { // must be relationtype
          basicFieldType =
              getSingletonBasicField((RelationType<FlexibleFieldSchema.Field>) ft.getType())
                  .map(field -> (BasicType) field.getTypes().get(0).getType())
                  .orElse(null);
          distanceAdjustment = 2; // we penalize matches into a singleton relation type
        }
        if (basicFieldType != null) {
          typeDistanceWithArray(
                  btype,
                  arrayDepth,
                  basicFieldType,
                  ft.getArrayDepth(),
                  force
                      ? settings.maxForceCastingTypeDistance()
                      : settings.maxCastingTypeDistance())
              .ifPresent(i -> potentialMatches.add(Pair.of(i * distanceAdjustment, ft)));
        }
      }
      match =
          potentialMatches.stream()
              .min(Comparator.comparing(Pair::getKey))
              .map(Pair::getValue)
              .orElse(null);
    }
    return match;
  }

  static Optional<FlexibleTableSchema.Field> getSingletonBasicField(
      RelationType<FlexibleTableSchema.Field> relationType) {
    if (relationType.fields.size() == 1) {
      var field = relationType.fields.get(0);
      if (field.getTypes().size() == 1) {
        var fieldType = field.getTypes().get(0);
        if (fieldType.getArrayDepth() == 0 && fieldType.getType() instanceof BasicType) {
          return Optional.of(field);
        }
      }
    }
    return Optional.empty();
  }

  private static final int ARRAY_DISTANCE_OFFSET = 100; // Assume maximum array depth is 100

  private Optional<Integer> typeDistanceWithArray(
      BasicType fromType,
      int fromArrayDepth,
      BasicType toType,
      int toArrayDepth,
      int maxTypeDistance) {
    if (fromArrayDepth > toArrayDepth
        || (!settings.deepenArrays() && fromArrayDepth != toArrayDepth)) {
      return Optional.empty();
    }
    // Type distance is the primary factor in determining match - array distance is secondary
    return typeDistance(fromType, toType, maxTypeDistance)
        .map(i -> i * ARRAY_DISTANCE_OFFSET + (toArrayDepth - fromArrayDepth));
  }

  private Optional<Integer> typeDistance(
      BasicType fromType, BasicType toType, int maxTypeDistance) {
    return BasicTypeManager.typeCastingDistance(fromType, toType)
        .filter(i -> i >= 0 && i <= maxTypeDistance);
  }
}
