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
package com.datasqrl.discovery.stats;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.SpecialName;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.constraint.Cardinality;
import com.datasqrl.io.schema.flexible.constraint.Constraint;
import com.datasqrl.io.schema.flexible.constraint.NotNull;
import com.datasqrl.io.schema.flexible.input.FlexibleFieldSchema;
import com.datasqrl.io.schema.flexible.input.FlexibleTableSchema;
import com.datasqrl.io.schema.flexible.input.FlexibleTypeMatcher;
import com.datasqrl.io.schema.flexible.input.RelationType;
import com.datasqrl.io.schema.flexible.input.SchemaAdjustmentSettings;
import com.datasqrl.io.schema.flexible.type.basic.BasicType;
import com.datasqrl.io.schema.flexible.type.basic.BasicTypeManager;
import com.datasqrl.io.schema.flexible.type.basic.StringType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.NonNull;

/** This class is not thread-safe and should be used to merge one schema at a time. */
public class DefaultSchemaGenerator extends FlexibleTypeMatcher implements SchemaGenerator {

  private boolean isComplete;

  public DefaultSchemaGenerator(SchemaAdjustmentSettings settings) {
    super(settings);
  }

  @Override
  public FlexibleTableSchema mergeSchema(
      @NonNull SourceTableStatistics tableStats,
      @NonNull FlexibleTableSchema tableDef,
      @NonNull ErrorCollector errors) {
    isComplete = !tableDef.isPartialSchema();
    var builder = new FlexibleTableSchema.Builder();
    builder.copyFrom(tableDef);
    builder.setPartialSchema(false);
    builder.setFields(merge(tableStats.relation, tableDef.getFields(), errors));
    return builder.build();
  }

  @Override
  public FlexibleTableSchema mergeSchema(
      @NonNull SourceTableStatistics tableStats,
      @NonNull Name tableName,
      @NonNull ErrorCollector errors) {
    return mergeSchema(tableStats, FlexibleTableSchema.empty(tableName), errors);
  }

  RelationType<FlexibleFieldSchema.Field> merge(
      @NonNull RelationStats relation,
      @NonNull RelationType<FlexibleFieldSchema.Field> fields,
      @NonNull ErrorCollector errors) {
    Set<Name> coveredNames = new HashSet<>();
    var numRecords = relation.getCount();
    RelationType.Builder<FlexibleFieldSchema.Field> builder = RelationType.build();
    for (FlexibleFieldSchema.Field f : fields) {
      builder.add(
          merge(
              relation.fieldStats.get(f.getName()),
              f,
              f.getName(),
              numRecords,
              errors.resolve(f.getName().getDisplay())));
      coveredNames.add(f.getName());
    }
    if (!isComplete) {
      for (Map.Entry<Name, FieldStats> e : relation.fieldStats.entrySet()) {
        if (!coveredNames.contains(e.getKey())) {
          builder.add(
              merge(
                  e.getValue(),
                  null,
                  e.getKey(),
                  numRecords,
                  errors.resolve(e.getKey().getDisplay())));
        }
      }
    }
    return builder.build();
  }

  FlexibleFieldSchema.Field merge(
      FieldStats fieldStats,
      FlexibleFieldSchema.Field fieldDef,
      @NonNull Name fieldName,
      long numRecords,
      @NonNull ErrorCollector errors) {
    Preconditions.checkArgument(fieldDef != null || !isComplete);
    var builder = new FlexibleFieldSchema.Field.Builder();
    if (fieldDef != null) {
      builder.copyFrom(fieldDef);
    } else {
      builder.setName(Name.changeDisplayName(fieldName, fieldStats.getDisplayName()));
    }
    var statsNotNull = false;
    if (fieldStats != null) {
      statsNotNull = fieldStats.numNulls == 0 && fieldStats.count == numRecords;
    }
    var types =
        merge(
            fieldStats != null ? fieldStats.types.keySet() : Collections.EMPTY_SET,
            fieldDef != null ? fieldDef.getTypes() : Collections.EMPTY_LIST,
            statsNotNull,
            fieldName,
            errors);
    builder.setTypes(types);
    return builder.build();
  }

  List<FlexibleFieldSchema.FieldType> merge(
      @NonNull Set<FieldTypeStats> statTypes,
      @NonNull List<FlexibleFieldSchema.FieldType> fieldTypes,
      boolean statsNotNull,
      @NonNull Name fieldName,
      @NonNull ErrorCollector errors) {
    if (fieldTypes.isEmpty()) {
      /* Need to generate single type from statistics. First, we check if there is one family of detected types.
        If not (or if there is ambiguity), we combine all of the raw types.
        This provides a defensive approach (i.e. we don't force type combination on detected types) with user friendliness
        in cases where the detected type is obvious.
      */
      errors.checkFatal(
          !isComplete, "Schema marked as complete but found additional field: %s", fieldName);
      FlexibleFieldSchema.FieldType result = null;
      List<Constraint> constraints =
          statsNotNull ? List.of(NotNull.INSTANCE) : Collections.EMPTY_LIST;
      var maxArrayDepth = 0;
      if (statTypes.isEmpty()) { // All field values where null, use String as default type
        Preconditions.checkArgument(!statsNotNull);
        result =
            new FlexibleFieldSchema.FieldType(
                SpecialName.SINGLETON, StringType.INSTANCE, maxArrayDepth, constraints);
      } else {
        BasicType type = null;
        for (FieldTypeStats fts : statTypes) {
          var td = fts.detected();
          if (td instanceof BasicType basicType) {
            if (type == null) {
              type = basicType;
            } else {
              type =
                  BasicTypeManager.combine(type, basicType, settings.maxCastingTypeDistance())
                      .orElse(null);
            }
            maxArrayDepth = Math.max(fts.arrayDepth(), maxArrayDepth);
          } else {
            type = null; // abort type finding since it's a nested relation
          }
          if (type == null) {
            break; // abort
          }
        }
        if (type != null) {
          // We have found a shared detected type
          result =
              new FlexibleFieldSchema.FieldType(
                  SpecialName.SINGLETON, type, maxArrayDepth, constraints);
        } else {
          // Combine all of the encountered raw types
          maxArrayDepth = 0;
          type = null;
          var nestedRelationArrayDepth = 0;
          RelationStats nested = null;
          for (FieldTypeStats fts : statTypes) {
            var td = fts.raw;
            if (td instanceof BasicType basicType) {
              if (type == null) {
                type = basicType;
              } else {
                type = BasicTypeManager.combineForced(type, basicType);
              }
              maxArrayDepth = Math.max(fts.arrayDepth(), maxArrayDepth);
            } else {
              assert fts.nestedRelationStats != null;
              if (nested == null) {
                nested = fts.nestedRelationStats.clone();
              } else {
                nested.merge(fts.nestedRelationStats);
              }
              nestedRelationArrayDepth = Math.max(nestedRelationArrayDepth, fts.arrayDepth());
            }
          }
          if (type != null) {
            result =
                new FlexibleFieldSchema.FieldType(
                    SpecialName.SINGLETON, type, maxArrayDepth, constraints);
          }
          if (nested != null) {
            RelationType<FlexibleFieldSchema.Field> nestedType =
                merge(nested, RelationType.EMPTY, errors);
            if (result != null) {
              // Need to embed basictype into nested relation as value
              var b = new FlexibleFieldSchema.Field.Builder();
              b.setName(SpecialName.VALUE);
              b.setTypes(Collections.singletonList(result));
              nestedType =
                  RelationType.<FlexibleFieldSchema.Field>build()
                      .addAll(nestedType)
                      .add(b.build())
                      .build();
            }
            if (nestedRelationArrayDepth == 0) {
              constraints = new ArrayList<>(constraints);
              constraints.add(new Cardinality(0, 1));
            }
            result =
                new FlexibleFieldSchema.FieldType(
                    SpecialName.SINGLETON, nestedType, 1, constraints);
          }
        }
      }
      assert result != null;
      return Collections.singletonList(result);
    } else {
      /*
        In this case, we need to honor the types as defined by the user in the schema. All we are doing here is checking
        that all of the types in the statistics have a place to match and alert the user if not (because that would lead to
        records being filtered out).
        We first try to match on raw type within type families with the closest relative. If that doesn't match, we try
        the same with the detected type. If all fails, we forcefully combine the raw type.
      */
      List<FlexibleFieldSchema.FieldType> result = new ArrayList<>(fieldTypes.size());
      Multimap<FlexibleFieldSchema.FieldType, FieldTypeStats> typePairing =
          ArrayListMultimap.create();
      for (FieldTypeStats fts : statTypes) {
        // Try to match on raw first
        var match = matchType(fts, fieldTypes);
        if (match != null) {
          if (!match.getType().getClass().equals(fts.raw().getClass())
              || match.getArrayDepth() != fts.arrayDepth()) {
            errors.notice("Matched field type [%s] onto [%s]", fts, match);
          }
          typePairing.put(match, fts);
        } else {
          errors.warn(
              "Cannot match field type [%s] onto defined schema. Such records will be ignored.",
              fts.raw);
        }
      }
      for (FlexibleFieldSchema.FieldType ft : fieldTypes) {
        result.add(merge(typePairing.get(ft), ft, errors));
      }
      return result;
    }
  }

  FlexibleFieldSchema.FieldType merge(
      @NonNull Collection<FieldTypeStats> ftstats,
      @NonNull FlexibleFieldSchema.FieldType ftdef,
      @NonNull ErrorCollector errors) {
    if (ftdef.getType() instanceof BasicType) {
      return ftdef; // It's an immutable object, no need to copy
    } else {
      RelationStats nested = null;
      for (FieldTypeStats fts : ftstats) {
        if (nested == null) {
          nested = fts.nestedRelationStats.clone();
        } else {
          nested.merge(fts.nestedRelationStats);
        }
      }
      return new FlexibleFieldSchema.FieldType(
          ftdef.getVariantName(),
          merge(
              nested == null ? RelationStats.EMPTY : nested,
              (RelationType) ftdef.getType(),
              errors),
          ftdef.getArrayDepth(),
          ftdef.getConstraints());
    }
  }
}
