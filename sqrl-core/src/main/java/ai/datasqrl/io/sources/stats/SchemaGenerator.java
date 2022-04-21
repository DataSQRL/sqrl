package ai.datasqrl.io.sources.stats;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.SpecialName;
import ai.datasqrl.schema.type.RelationType;
import ai.datasqrl.schema.type.Type;
import ai.datasqrl.schema.type.basic.BasicType;
import ai.datasqrl.schema.type.basic.BasicTypeManager;
import ai.datasqrl.schema.type.basic.StringType;
import ai.datasqrl.schema.type.constraint.Cardinality;
import ai.datasqrl.schema.type.constraint.Constraint;
import ai.datasqrl.schema.type.schema.FlexibleDatasetSchema;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.ImmutablePair;

public class SchemaGenerator {

  private boolean isComplete;

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
    RelationType.Builder<FlexibleDatasetSchema.FlexibleField> builder = RelationType.build();
    for (FlexibleDatasetSchema.FlexibleField f : fields) {
      builder.add(
          merge(relation.fieldStats.get(f.getName()), f, f.getName(), errors.resolve(f.getName())));
      coveredNames.add(f.getName());
    }
    if (!isComplete) {
      for (Map.Entry<Name, FieldStats> e : relation.fieldStats.entrySet()) {
        if (!coveredNames.contains(e.getKey())) {
          builder.add(merge(e.getValue(), null, e.getKey(), errors.resolve(e.getKey())));
        }
      }
    }
    return builder.build();
  }

  FlexibleDatasetSchema.FlexibleField merge(FieldStats fieldStats,
      FlexibleDatasetSchema.FlexibleField fieldDef,
      @NonNull Name fieldName, @NonNull ErrorCollector errors) {
    Preconditions.checkArgument(fieldDef != null || !isComplete);
    FlexibleDatasetSchema.FlexibleField.Builder builder = new FlexibleDatasetSchema.FlexibleField.Builder();
    if (fieldDef != null) {
      builder.copyFrom(fieldDef);
    } else {
      builder.setName(Name.changeDisplayName(fieldName, fieldStats.getDisplayName()));
    }
    List<FlexibleDatasetSchema.FieldType> types = merge(
        fieldStats != null ? fieldStats.types.keySet() : Collections.EMPTY_SET,
        fieldDef != null ? fieldDef.getTypes() : Collections.EMPTY_LIST, errors);
    builder.setTypes(types);
    return builder.build();
  }

  List<FlexibleDatasetSchema.FieldType> merge(@NonNull Set<FieldTypeStats> statTypes,
      @NonNull List<FlexibleDatasetSchema.FieldType> fieldTypes, @NonNull ErrorCollector errors) {
    if (fieldTypes.isEmpty()) {
            /* Need to generate single type from statistics. First, we check if there is one family of detected types.
               If not (or if there is ambiguity), we combine all of the raw types.
               This provides a defensive approach (i.e. we don't force type combination on detected types) with user friendliness
               in cases where the detected type is obvious.
             */
      Preconditions.checkArgument(!statTypes.isEmpty() && !isComplete);
      FlexibleDatasetSchema.FieldType result = null;
      int maxArrayDepth = 0;
      BasicType type = null;
      for (FieldTypeStats fts : statTypes) {
        Type td = fts.getDetected();
        if (td instanceof BasicType) {
          if (type == null) {
            type = (BasicType) td;
          } else {
            type = BasicTypeManager.combine(type, (BasicType) td, false);
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
            Collections.EMPTY_LIST);
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
              type = BasicTypeManager.combine(type, (BasicType) td, true);
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
              Collections.EMPTY_LIST);
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
          List<Constraint> constraints = Collections.EMPTY_LIST;
          if (nestedRelationArrayDepth == 0) {
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
               We first try to match on raw type witin type families with the closest relative. If that doesn't match, we try
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

//    public static FlexibleDatasetSchema.FieldType matchType(FieldTypeStats.TypeDepth rawType, FieldTypeStats.TypeDepth detectedType,
//                                                             List<FlexibleDatasetSchema.FieldType> fieldTypes) {
//        return matchType(rawType.getType(), rawType.getArrayDepth(), detectedType.getType(), detectedType.getArrayDepth(), fieldTypes);
//    }

  public static FlexibleDatasetSchema.FieldType matchType(TypeSignature typeSignature,
      List<FlexibleDatasetSchema.FieldType> fieldTypes) {
    FlexibleDatasetSchema.FieldType match;
    //First, try to match raw type
    match = matchSingleType(typeSignature.getRaw(), typeSignature.getArrayDepth(), fieldTypes);
    if (match == null) {
      //Second, try to match on detected
      match = matchSingleType(typeSignature.getDetected(), typeSignature.getArrayDepth(),
          fieldTypes);
      if (match == null) {
        //If neither of those worked, try to force a match which means casting raw to STRING if available
        match = fieldTypes.stream().filter(ft -> typeSignature.getArrayDepth() <= ft.getArrayDepth()
                && ft.getType() instanceof StringType)
            .min(Comparator.comparing(FlexibleDatasetSchema.FieldType::getArrayDepth)).orElse(null);
      }
    }
    return match;
  }

  private static FlexibleDatasetSchema.FieldType matchSingleType(Type type, int arrayDepth,
      List<FlexibleDatasetSchema.FieldType> fieldTypes) {
    if (type instanceof RelationType) {
      assert arrayDepth == 1;
      return fieldTypes.stream().filter(ft -> ft.getType() instanceof RelationType).findFirst()
          .orElse(null);
    } else {
      BasicType btype = (BasicType) type;
      return fieldTypes.stream().filter(ft -> ft.getType() instanceof BasicType)
          .map(ft -> new ImmutablePair<>(
              typeDistance(btype, arrayDepth, (BasicType) ft.getType(), ft.getArrayDepth()), ft))
          .filter(p -> p.getKey() >= 0).min(Comparator.comparing(ImmutablePair::getKey))
          .map(ImmutablePair::getValue).orElse(null);
    }
  }

  private static final int ARRAY_DISTANCE_OFFSET = 100; //Assume maximum array depth is 100

  public static int typeDistance(BasicType childType, int childArrayDepth, BasicType ancestorType,
      int ancestorArrayDepth) {
    if (childArrayDepth > ancestorArrayDepth) {
      return -1;
    }
    return typeDistance(childType, ancestorType) * ARRAY_DISTANCE_OFFSET + (ancestorArrayDepth
        - childArrayDepth);
  }

  private static final int SIBLING_DISTANCE_OFFSET = 5;

  public static int typeDistance(BasicType baseType, BasicType relatedType) {
    BasicType parent = baseType;
    int distance = 0;
    Map<BasicType, Integer> distanceMap = new HashMap<>();
    while (parent != null && !parent.equals(relatedType)) {
      distanceMap.put(parent, distance);
      parent = parent.parentType();
      distance++;
    }
    if (parent
        == null) { //We did not find a match within the ancestors. Let's see if it's a sibling
      parent = relatedType.parentType();
      distance = 1;
      while (parent != null && !distanceMap.containsKey(parent)) {
        parent = parent.parentType();
        distance++;
      }
      if (parent == null) { //Could not find a path between the two types in the type hierarchy
        return -1;
      } else {
        return distance * SIBLING_DISTANCE_OFFSET + distanceMap.get(parent);
      }
    } else {
      return distance;
    }
  }

}
