package com.datasqrl.io.stats;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.name.Name;
import com.datasqrl.name.SpecialName;
import com.datasqrl.schema.constraint.Cardinality;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.constraint.NotNull;
import com.datasqrl.schema.input.FlexibleDatasetSchema;
import com.datasqrl.schema.input.RelationType;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.datasqrl.schema.type.Type;
import com.datasqrl.schema.type.basic.BasicType;
import com.datasqrl.schema.type.basic.BasicTypeManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.*;

/**
 * This class is not thread-safe and should be used to merge one schema at a time.
 */
public class SchemaGenerator implements Serializable {

  private final SchemaAdjustmentSettings settings;

  private boolean isComplete;

  public SchemaGenerator(SchemaAdjustmentSettings settings) {
    this.settings = settings;
  }

  public FlexibleDatasetSchema.TableField mergeSchema(@NonNull SourceTableStatistics tableStats,
      @NonNull FlexibleDatasetSchema.TableField tableDef, @NonNull ErrorCollector errors) {
    isComplete = !tableDef.isPartialSchema();
    FlexibleDatasetSchema.TableField.Builder builder = new FlexibleDatasetSchema.TableField.Builder();
    builder.copyFrom(tableDef);
    builder.setPartialSchema(false);
    builder.setFields(merge(tableStats.relation, tableDef.getFields(), errors));
    return builder.build();
  }

  public FlexibleDatasetSchema.TableField mergeSchema(@NonNull SourceTableStatistics tableStats,
      @NonNull Name tableName, @NonNull ErrorCollector errors) {
    return mergeSchema(tableStats, FlexibleDatasetSchema.TableField.empty(tableName), errors);
  }

  RelationType<FlexibleDatasetSchema.FlexibleField> merge(@NonNull RelationStats relation,
      @NonNull RelationType<FlexibleDatasetSchema.FlexibleField> fields,
      @NonNull ErrorCollector errors) {
    Set<Name> coveredNames = new HashSet<>();
    long numRecords = relation.getCount();
    RelationType.Builder<FlexibleDatasetSchema.FlexibleField> builder = RelationType.build();
    for (FlexibleDatasetSchema.FlexibleField f : fields) {
      builder.add(
          merge(relation.fieldStats.get(f.getName()), f, f.getName(), numRecords,
              errors.resolve(f.getName())));
      coveredNames.add(f.getName());
    }
    if (!isComplete) {
      for (Map.Entry<Name, FieldStats> e : relation.fieldStats.entrySet()) {
        if (!coveredNames.contains(e.getKey())) {
          builder.add(
              merge(e.getValue(), null, e.getKey(), numRecords, errors.resolve(e.getKey())));
        }
      }
    }
    return builder.build();
  }

  FlexibleDatasetSchema.FlexibleField merge(FieldStats fieldStats,
      FlexibleDatasetSchema.FlexibleField fieldDef,
      @NonNull Name fieldName, long numRecords, @NonNull ErrorCollector errors) {
    Preconditions.checkArgument(fieldDef != null || !isComplete);
    FlexibleDatasetSchema.FlexibleField.Builder builder = new FlexibleDatasetSchema.FlexibleField.Builder();
    if (fieldDef != null) {
      builder.copyFrom(fieldDef);
    } else {
      builder.setName(Name.changeDisplayName(fieldName, fieldStats.getDisplayName()));
    }
    boolean statsNotNull = false;
    if (fieldStats != null) {
      statsNotNull = fieldStats.numNulls == 0 && fieldStats.count == numRecords;
    }
    List<FlexibleDatasetSchema.FieldType> types = merge(
        fieldStats != null ? fieldStats.types.keySet() : Collections.EMPTY_SET,
        fieldDef != null ? fieldDef.getTypes() : Collections.EMPTY_LIST, statsNotNull, errors);
    builder.setTypes(types);
    return builder.build();
  }

  List<FlexibleDatasetSchema.FieldType> merge(@NonNull Set<FieldTypeStats> statTypes,
      @NonNull List<FlexibleDatasetSchema.FieldType> fieldTypes, boolean statsNotNull,
      @NonNull ErrorCollector errors) {
    if (fieldTypes.isEmpty()) {
      /* Need to generate single type from statistics. First, we check if there is one family of detected types.
         If not (or if there is ambiguity), we combine all of the raw types.
         This provides a defensive approach (i.e. we don't force type combination on detected types) with user friendliness
         in cases where the detected type is obvious.
       */
      Preconditions.checkArgument(!statTypes.isEmpty() && !isComplete);
      FlexibleDatasetSchema.FieldType result = null;
      List<Constraint> constraints =
          statsNotNull ? List.of(NotNull.INSTANCE) : Collections.EMPTY_LIST;
      int maxArrayDepth = 0;
      BasicType type = null;
      for (FieldTypeStats fts : statTypes) {
        Type td = fts.getDetected();
        if (td instanceof BasicType) {
          if (type == null) {
            type = (BasicType) td;
          } else {
            type = BasicTypeManager.combine(type, (BasicType) td, settings.maxCastingTypeDistance())
                .orElse(null);
          }
          maxArrayDepth = Math.max(fts.getArrayDepth(), maxArrayDepth);
        } else {
          type = null; //abort type finding since it's a nested relation
        }
        if (type == null) {
          break; //abort
        }
      }
      if (type != null) {
        //We have found a shared detected type
        result = new FlexibleDatasetSchema.FieldType(SpecialName.SINGLETON, type, maxArrayDepth,
            constraints);
      } else {
        //Combine all of the encountered raw types
        maxArrayDepth = 0;
        type = null;
        int nestedRelationArrayDepth = 0;
        RelationStats nested = null;
        for (FieldTypeStats fts : statTypes) {
          Type td = fts.raw;
          if (td instanceof BasicType) {
            if (type == null) {
              type = (BasicType) td;
            } else {
              type = BasicTypeManager.combineForced(type, (BasicType) td);
            }
            maxArrayDepth = Math.max(fts.getArrayDepth(), maxArrayDepth);
          } else {
            assert fts.nestedRelationStats != null;
            if (nested == null) {
              nested = fts.nestedRelationStats.clone();
            } else {
              nested.merge(fts.nestedRelationStats);
            }
            nestedRelationArrayDepth = Math.max(nestedRelationArrayDepth, fts.getArrayDepth());
          }
        }
        if (type != null) {
          result = new FlexibleDatasetSchema.FieldType(SpecialName.SINGLETON, type, maxArrayDepth,
              constraints);
        }
        if (nested != null) {
          RelationType<FlexibleDatasetSchema.FlexibleField> nestedType = merge(nested,
              RelationType.EMPTY, errors);
          if (result != null) {
            //Need to embed basictype into nested relation as value
            FlexibleDatasetSchema.FlexibleField.Builder b = new FlexibleDatasetSchema.FlexibleField.Builder();
            b.setName(SpecialName.VALUE);
            b.setTypes(Collections.singletonList(result));
            nestedType = RelationType.<FlexibleDatasetSchema.FlexibleField>build()
                .addAll(nestedType)
                .add(b.build())
                .build();
          }
          if (nestedRelationArrayDepth == 0) {
            constraints = new ArrayList<>(constraints);
            constraints.add(new Cardinality(0, 1));
          }
          result = new FlexibleDatasetSchema.FieldType(SpecialName.SINGLETON, nestedType,
              1, constraints);
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
      List<FlexibleDatasetSchema.FieldType> result = new ArrayList<>(fieldTypes.size());
      Multimap<FlexibleDatasetSchema.FieldType, FieldTypeStats> typePairing = ArrayListMultimap.create();
      for (FieldTypeStats fts : statTypes) {
        //Try to match on raw first
        FlexibleDatasetSchema.FieldType match = matchType(fts, fieldTypes);
        if (match != null) {
          if (!match.getType().getClass().equals(fts.getRaw().getClass())
              || match.getArrayDepth() != fts.getArrayDepth()) {
            errors.notice("Matched field type [%s] onto [%s]", fts, match);
          }
          typePairing.put(match, fts);
        } else {
          errors.warn(
              "Cannot match field type [%s] onto defined schema. Such records will be ignored.",
              fts.raw);
        }
      }
      for (FlexibleDatasetSchema.FieldType ft : fieldTypes) {
        result.add(merge(typePairing.get(ft), ft, errors));
      }
      return result;
    }
  }

  FlexibleDatasetSchema.FieldType merge(@NonNull Collection<FieldTypeStats> ftstats,
      @NonNull FlexibleDatasetSchema.FieldType ftdef, @NonNull ErrorCollector errors) {
    if (ftdef.getType() instanceof BasicType) {
      return ftdef; //It's an immutable object, no need to copy
    } else {
      RelationStats nested = null;
      for (FieldTypeStats fts : ftstats) {
        if (nested == null) {
          nested = fts.nestedRelationStats.clone();
        } else {
          nested.merge(fts.nestedRelationStats);
        }
      }
      return new FlexibleDatasetSchema.FieldType(ftdef.getVariantName(),
          merge(nested == null ? RelationStats.EMPTY : nested, (RelationType) ftdef.getType(),
              errors),
          ftdef.getArrayDepth(), ftdef.getConstraints());
    }
  }

  public FlexibleDatasetSchema.FieldType matchType(TypeSignature typeSignature,
      List<FlexibleDatasetSchema.FieldType> fieldTypes) {
    FlexibleDatasetSchema.FieldType match;
    //First, try to match raw type
    match = matchSingleType(typeSignature.getRaw(), typeSignature.getArrayDepth(), fieldTypes,
        false);
    if (match == null) {
      //Second, try to match on detected
      match = matchSingleType(typeSignature.getDetected(), typeSignature.getArrayDepth(),
          fieldTypes, false);
      if (match == null) {
        //If neither of those worked, try to force a match which means casting raw to STRING if available
        match = matchSingleType(typeSignature.getRaw(), typeSignature.getArrayDepth(), fieldTypes,
            true);
      }
    }
    return match;
  }

  private FlexibleDatasetSchema.FieldType matchSingleType(Type type, int arrayDepth,
      List<FlexibleDatasetSchema.FieldType> fieldTypes, boolean force) {
    FlexibleDatasetSchema.FieldType match;
    if (type instanceof RelationType) {
      assert arrayDepth == 1;
      match = fieldTypes.stream().filter(ft -> ft.getType() instanceof RelationType).findFirst()
          .orElse(null);
      if (match == null && force) {
        //TODO: Should we consider coercing a relation to string (as json)?
      }
    } else {
      BasicType btype = (BasicType) type;
      List<Pair<Integer, FlexibleDatasetSchema.FieldType>> potentialMatches = new ArrayList<>(
          fieldTypes.size());
      for (FlexibleDatasetSchema.FieldType ft : fieldTypes) {
        if (ft.getType() instanceof BasicType) {
          typeDistanceWithArray(btype, arrayDepth, (BasicType) ft.getType(), ft.getArrayDepth(),
              force ? settings.maxForceCastingTypeDistance() : settings.maxCastingTypeDistance())
              .ifPresent(i -> potentialMatches.add(Pair.of(i, ft)));
        }
      }
      match = potentialMatches.stream().min(Comparator.comparing(Pair::getKey))
          .map(Pair::getValue).orElse(null);
    }
    return match;
  }

  private static final int ARRAY_DISTANCE_OFFSET = 100; //Assume maximum array depth is 100

  private Optional<Integer> typeDistanceWithArray(BasicType fromType, int fromArrayDepth,
      BasicType toType,
      int toArrayDepth, int maxTypeDistance) {
    if (fromArrayDepth > toArrayDepth || (!settings.deepenArrays()
        && fromArrayDepth != toArrayDepth)) {
      return Optional.empty();
    }
    //Type distance is the primary factor in determining match - array distance is secondary
    return typeDistance(fromType, toType, maxTypeDistance)
        .map(i -> i * ARRAY_DISTANCE_OFFSET + (toArrayDepth - fromArrayDepth));
  }

  private Optional<Integer> typeDistance(BasicType fromType, BasicType toType,
      int maxTypeDistance) {
    return BasicTypeManager.typeCastingDistance(fromType, toType)
        .filter(i -> i >= 0 && i <= maxTypeDistance);
  }

}
