package ai.dataeng.sqml.ingest.stats;

import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.basic.BasicTypeManager;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.basic.StringType;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NamePath;
import ai.dataeng.sqml.schema2.name.SpecialName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.*;

public class SchemaGenerator {

    private ConversionError.Bundle<SchemaConversionError> errors = new ConversionError.Bundle<>();
    private boolean isComplete;

    public FlexibleDatasetSchema.TableField mergeSchema(@NonNull SourceTableStatistics tableStats, @NonNull FlexibleDatasetSchema.TableField tableDef, @NonNull NamePath location) {
        isComplete = !tableDef.isPartialSchema();
        FlexibleDatasetSchema.TableField.Builder builder = new FlexibleDatasetSchema.TableField.Builder();
        builder.copyFrom(tableDef);
        builder.setPartialSchema(false);
        builder.setFields(merge(tableStats.relation, tableDef.getFields(), location));
        return builder.build();
    }

    public FlexibleDatasetSchema.TableField mergeSchema(@NonNull SourceTableStatistics tableStats, @NonNull Name tableName, @NonNull NamePath location) {
        return mergeSchema(tableStats, FlexibleDatasetSchema.TableField.empty(tableName), location);
    }

    RelationType<FlexibleDatasetSchema.FlexibleField> merge(@NonNull RelationStats relation, @NonNull RelationType<FlexibleDatasetSchema.FlexibleField> fields, @NonNull NamePath location) {
        Set<Name> coveredNames = new HashSet<>();
        RelationType.Builder<FlexibleDatasetSchema.FlexibleField> builder = RelationType.<FlexibleDatasetSchema.FlexibleField>build();
        for (FlexibleDatasetSchema.FlexibleField f : fields) {
            builder.add(merge(relation.fieldStats.get(f.getName()),f,location.resolve(f.getName())));
            coveredNames.add(f.getName());
        }
        if (!isComplete) {
            for (Map.Entry<Name,FieldStats> e : relation.fieldStats.entrySet()) {
                if (!coveredNames.contains(e.getKey())) {
                    builder.add(merge(e.getValue(),null,location.resolve(e.getKey())));
                }
            }
        }
        return builder.build();
    }

    FlexibleDatasetSchema.FlexibleField merge(FieldStats fieldStats, FlexibleDatasetSchema.FlexibleField fieldDef, @NonNull NamePath location) {
        Preconditions.checkArgument(fieldDef!=null || !isComplete);
        FlexibleDatasetSchema.FlexibleField.Builder builder = new FlexibleDatasetSchema.FlexibleField.Builder();
        if (fieldDef!=null) builder.copyFrom(fieldDef);
        else {
            builder.setName(Name.changeDisplayName(location.getLast(),fieldStats.getDisplayName()));
        }
        List<FlexibleDatasetSchema.FieldType> types = merge(
                fieldStats!=null?fieldStats.types.keySet():Collections.EMPTY_SET,
                fieldDef!=null?fieldDef.getTypes():Collections.EMPTY_LIST,location);
        builder.setTypes(types);
        return builder.build();
    }

    List<FlexibleDatasetSchema.FieldType> merge(@NonNull Set<FieldTypeStats> statTypes, @NonNull List<FlexibleDatasetSchema.FieldType> fieldTypes, @NonNull NamePath location) {
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
                FieldTypeStats.TypeDepth td = fts.detected;
                if (td.isBasic()) {
                    if (type==null) type = td.getBasicType();
                    else type = BasicTypeManager.combine(type,td.getBasicType(),false);
                    maxArrayDepth = Math.max(td.getArrayDepth(),maxArrayDepth);
                } else {
                    type = null; //abort type finding since it's a nested relation
                    break;
                }
                if (type == null) break; //abort
            }
            if (type!=null) {
                //We have found a shared detected type
                result = new FlexibleDatasetSchema.FieldType(SpecialName.SINGLETON,type,maxArrayDepth,Collections.EMPTY_LIST);
            } else {
                //Combine all of the encountered raw types
                maxArrayDepth = 0;
                type = null;
                RelationStats nested = null;
                for (FieldTypeStats fts : statTypes) {
                    FieldTypeStats.TypeDepth td = fts.raw;
                    if (td.isBasic()) {
                        if (type==null) type = td.getBasicType();
                        else type = BasicTypeManager.combine(type,td.getBasicType(),true);
                        maxArrayDepth = Math.max(td.getArrayDepth(),maxArrayDepth);
                    } else {
                        assert fts.nestedRelationStats!=null;
                        if (nested == null) nested = fts.nestedRelationStats.clone();
                        else nested.merge(fts.nestedRelationStats);
                    }
                }
                if (type!=null) {
                    result = new FlexibleDatasetSchema.FieldType(SpecialName.SINGLETON,type,maxArrayDepth,Collections.EMPTY_LIST);
                }
                if (nested!=null) {
                    RelationType<FlexibleDatasetSchema.FlexibleField> nestedType = merge(nested, RelationType.EMPTY, location);
                    if (result!=null) {
                        //Need to embed basictype into nested relation as value
                        FlexibleDatasetSchema.FlexibleField.Builder b = new FlexibleDatasetSchema.FlexibleField.Builder();
                        b.setName(SpecialName.VALUE);
                        b.setTypes(Collections.singletonList(result));
                        nestedType = RelationType.<FlexibleDatasetSchema.FlexibleField>build()
                                .addAll(nestedType)
                                .add(b.build())
                                .build();
                    }
                    result = new FlexibleDatasetSchema.FieldType(SpecialName.SINGLETON,nestedType,
                            1,Collections.EMPTY_LIST);
                }
            }
            assert result!=null;
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
                FlexibleDatasetSchema.FieldType match = matchType(fts.raw, fieldTypes, false);
                if (match!=null) typePairing.put(match,fts);
                else {
                    //Try to match on detected
                    match = matchType(fts.detected, fieldTypes, false);
                    if (match!=null) typePairing.put(match,fts);
                    else {
                        //Force a match
                        match = matchType(fts.raw, fieldTypes, true);
                        if (match!=null) typePairing.put(match,fts);
                        else addError(SchemaConversionError.warn(location,"Cannot match field type [%s] onto defined schema. Such records will be ignored.",fts.raw));
                    }
                }
            }
            for (FlexibleDatasetSchema.FieldType ft : fieldTypes) {
                result.add(merge(typePairing.get(ft),ft,location));
            }
            return result;
        }
    }

    FlexibleDatasetSchema.FieldType merge(@NonNull Collection<FieldTypeStats> ftstats, @NonNull FlexibleDatasetSchema.FieldType ftdef, @NonNull NamePath location) {
        if (ftdef.getType() instanceof BasicType) return ftdef; //It's an immutable object, no need to copy
        else {
            RelationStats nested = null;
            for (FieldTypeStats fts : ftstats) {
                if (nested==null) nested = fts.nestedRelationStats.clone();
                else nested.merge(fts.nestedRelationStats);
            }
            return new FlexibleDatasetSchema.FieldType(ftdef.getVariantName(),
                    merge(nested==null?RelationStats.EMPTY:nested,(RelationType)ftdef.getType(),location),
                    ftdef.getArrayDepth(),ftdef.getConstraints());
        }
    }

    public static FlexibleDatasetSchema.FieldType matchType(FieldTypeStats.TypeDepth typeDepth, List<FlexibleDatasetSchema.FieldType> fieldTypes, boolean force) {
        return matchType(typeDepth.getType(),typeDepth.getArrayDepth(),fieldTypes, force);
    }

    public static FlexibleDatasetSchema.FieldType matchType(Type type, int arrayDepth, List<FlexibleDatasetSchema.FieldType> fieldTypes, boolean force) {
        if (type instanceof RelationType) {
            assert arrayDepth==1;
            return fieldTypes.stream().filter(ft -> ft.getType() instanceof RelationType).findFirst().orElse(null);
        } else {
            BasicType btype = (BasicType) type;
            return fieldTypes.stream().filter(ft -> ft.getType() instanceof BasicType)
                    .map(ft -> new ImmutablePair<>(typeDistance(btype, arrayDepth, (BasicType)ft.getType(), ft.getArrayDepth(), force),ft))
                    .filter(p -> p.getKey()>=0).min(Comparator.comparing(ImmutablePair::getKey)).map(ImmutablePair::getValue).orElse(null);
        }
    }

    private static final int DISTANCE_PADDING = 1000;

    public static int typeDistance(BasicType childType, int childArrayDepth, BasicType ancestorType, int ancestorArrayDepth, boolean force) {
        if (childArrayDepth>ancestorArrayDepth) return -1;
        return typeDistance(childType,ancestorType, force)*DISTANCE_PADDING + (ancestorArrayDepth-childArrayDepth);
    }

    public static int typeDistance(BasicType childType, BasicType ancestorType, boolean force) {
        BasicType parent = childType;
        int distance = 0;
        while (parent!=null && !parent.equals(ancestorType)) {
            parent = parent.parentType();
            distance++;
        }
        if (parent==null) { //We did not find a match within the type hierarchy
            if (force) {
                //TODO: Cast to sibling

                //Final fallback: cast up to STRING
                if (ancestorType instanceof StringType) return DISTANCE_PADDING;
            }
            return -1;
        } else {
            return distance;
        }
    }


    public boolean hasErrors() {
        return errors.hasErrors();
    }

    public ConversionError.Bundle<SchemaConversionError> getErrors() {
        return errors;
    }

    private void addError(SchemaConversionError error) {
        errors.add(error);
    }
}
