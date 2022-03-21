package ai.dataeng.sqml.type.schema;

import ai.dataeng.sqml.io.sources.SourceRecord;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import ai.dataeng.sqml.io.sources.stats.FieldStats;
import ai.dataeng.sqml.io.sources.stats.SchemaGenerator;
import ai.dataeng.sqml.io.sources.stats.TypeSignature;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.basic.BasicType;
import ai.dataeng.sqml.type.basic.ConversionResult;
import ai.dataeng.sqml.type.basic.ProcessMessage.ProcessBundle;
import ai.dataeng.sqml.type.basic.StringType;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Follows {@link ai.dataeng.sqml.io.sources.stats.SchemaGenerator} in structure and semantics.
 */
public class SchemaValidator implements Serializable {

    private final SchemaAdjustmentSettings settings;
    private final FlexibleDatasetSchema.TableField tableSchema;
    private final NameCanonicalizer canonicalizer;

    public SchemaValidator(@NonNull FlexibleDatasetSchema.TableField tableSchema, @NonNull SchemaAdjustmentSettings settings,
                           @NonNull SourceDataset.Digest dataset) {
        Preconditions.checkArgument(!tableSchema.isPartialSchema());
        this.settings = settings;
        this.tableSchema = tableSchema;
        this.canonicalizer = dataset.getCanonicalizer();
    }


    public SourceRecord.Named verifyAndAdjust(SourceRecord<String> record, ProcessBundle<SchemaConversionError> errors) {
        Map<Name,Object> result = verifyAndAdjust(record.getData(), tableSchema.getFields(), NamePath.ROOT, errors);
        return record.replaceData(result);
    }

    private Map<Name,Object> verifyAndAdjust(Map<String, Object> relationData, RelationType<FlexibleDatasetSchema.FlexibleField> relationSchema,
                                             NamePath location, ProcessBundle<SchemaConversionError> errors) {
        Map<Name,Object> result = new HashMap<>(relationData.size());
        Set<Name> visitedFields = new HashSet<>();
        for (Map.Entry<String,Object> entry : relationData.entrySet()) {
            Name name = Name.of(entry.getKey(), canonicalizer);
            Object data = entry.getValue();
            FlexibleDatasetSchema.FlexibleField field = relationSchema.getFieldByName(name);
            if (field==null) {
                if (!settings.dropFields()) {
                    errors.add(SchemaConversionError.fatal(location,"Field is not defined in schema: %s", field));
                }
            } else {
                Pair<Name,Object> fieldResult = null;
                if (data != null) {
                    fieldResult = verifyAndAdjust(data, field, location.resolve(name), errors);
                }
                if (fieldResult == null && isNonNull(field)) {
                    fieldResult = handleNull(field, location, errors);
                }
                if (fieldResult!=null) result.put(fieldResult.getKey(),fieldResult.getValue());
            }
            visitedFields.add(name);
        }

        //See if we missed any non-null fields
        for (FlexibleDatasetSchema.FlexibleField field : relationSchema.getFields()) {
            if (!visitedFields.contains(field.getName()) && isNonNull(field)) {
                Pair<Name,Object> fieldResult = handleNull(field, location, errors);
                if (fieldResult!=null) result.put(fieldResult.getKey(),fieldResult.getValue());
            }
        }
        return result;
    }

    private boolean isNonNull(FlexibleDatasetSchema.FlexibleField field) {
        //Use memoization to reduce repeated computation
        return FlexibleSchemaHelper.isNonNull(field);
    }

    private Pair<Name,Object> handleNull(FlexibleDatasetSchema.FlexibleField field, NamePath location,
                                         ProcessBundle<SchemaConversionError> errors) {
        //See if we can map this onto any field type
        for (FlexibleDatasetSchema.FieldType ft : field.getTypes()) {
            if (ft.getArrayDepth()>0 && settings.null2EmptyArray()) {
                return ImmutablePair.of(FlexibleSchemaHelper.getCombinedName(field,ft),
                        deepenArray(Collections.EMPTY_LIST, ft.getArrayDepth()-1));
            }
        }
        errors.add(SchemaConversionError.fatal(location,"Field [%s] has non-null constraint but record contains null value", field));
        return null;
    }

    private static BasicType detectType(Map<String, Object> originalComposite, List<FlexibleDatasetSchema.FieldType> ftypes) {
        return detectTypeInternal((t, d) -> t.conversion().detectType(d),originalComposite,ftypes);
    }

    private static BasicType detectType(String original, List<FlexibleDatasetSchema.FieldType> ftypes) {
        return detectTypeInternal((t, d) -> t.conversion().detectType(d),original,ftypes);
    }

    private static<O> BasicType detectTypeInternal(BiPredicate<BasicType, O> typeFilter, O data, List<FlexibleDatasetSchema.FieldType> ftypes) {
        return ftypes.stream().filter(t -> t.getType() instanceof BasicType).map(t -> (BasicType)t.getType())
                .filter(t -> typeFilter.test(t,data)).findFirst().orElse(null);
    }

    private Pair<Name,Object> verifyAndAdjust(Object data, FlexibleDatasetSchema.FlexibleField field,
                                              NamePath location, ProcessBundle<SchemaConversionError> errors) {
        List<FlexibleDatasetSchema.FieldType> types = field.getTypes();
        TypeSignature.Simple typeSignature = FieldStats.detectTypeSignature(data, s -> detectType(s,types),
                m -> detectType(m,types));
        FlexibleDatasetSchema.FieldType match = SchemaGenerator.matchType(typeSignature, types);
        if (match != null) {
            Object converted = verifyAndAdjust(data, match, field, typeSignature.getArrayDepth(), location, errors);
            return ImmutablePair.of(FlexibleSchemaHelper.getCombinedName(field,match),converted);
        } else {
            errors.add(SchemaConversionError.notice(location, "Cannot match field data [%s] onto schema field [%s], hence field is ignored", data, field));
            return null;
        }
    }

    private Object convertDataToMatchedType(Object data, Type type, FlexibleDatasetSchema.FlexibleField field,
                                            NamePath location, ProcessBundle<SchemaConversionError> errors) {
        if (FieldStats.isArray(data)) {
            Collection<Object> col =  FieldStats.array2Collection(data);
            List<Object> result = new ArrayList<>(col.size());
            for (Object o : col) {
                if (o == null) {
                    if (!settings.removeListNulls()) {
                        errors.add(SchemaConversionError.fatal(location,"Array contains null values: [%s]", col));
                    }
                } else {
                    result.add(convertDataToMatchedType(o, type, field, location, errors));
                }
            }
            return result;
        } else {
            if (type instanceof RelationType) {
                if (data instanceof Map) {
                    return verifyAndAdjust((Map)data,(RelationType)type,location, errors);
                } else {
                    errors.add(SchemaConversionError.fatal(location,"Expected composite object [%s]", data));
                    return null;
                }
            } else {
                assert type instanceof BasicType;
                return cast2BasicType(data, (BasicType) type,location,errors);
            }
        }
    }

    private Object cast2BasicType(Object data, BasicType type,
                                         NamePath location, ProcessBundle<SchemaConversionError> errors) {
        if (type instanceof StringType) {
            if (data instanceof String) {
                return data;
            } else { //Cast to string
                if (!settings.castDataType()) {
                    errors.add(SchemaConversionError.fatal(location,"Expected string but got: %s",data));
                }
                return data.toString();
            }
        }
        if (data instanceof String || data instanceof Map) {
            //The datatype was detected
            if (!settings.castDataType()) {
                errors.add(SchemaConversionError.fatal(location,"Encountered [%s] but expected [%s]", data, type));
            }
            ConversionResult conversionResult = type.conversion().parseDetected(data);
            if (conversionResult.hasError()) {
                errors.add(SchemaConversionError.fatal(location,"Could not parse [%s] for type [%s]", data, type));
                return null;
            } else {
                data = conversionResult.getResult();
            }
        }
        try {
            return type.conversion().convert(data);
        } catch (IllegalArgumentException e) {
            errors.add(SchemaConversionError.fatal(location,"Could not convert [%s] to type [%s]", data, type));
            return null;
        }
    }

    private Object deepenArray(Object data, int additionalDepth) {
        Preconditions.checkNotNull(data);
        Preconditions.checkNotNull(additionalDepth>=0);
        for (int i = 0; i < additionalDepth; i++) {
            data = Collections.singletonList(data);
        }
        return data;
    }

    private Object verifyAndAdjust(Object data, FlexibleDatasetSchema.FieldType type, FlexibleDatasetSchema.FlexibleField field,
                                   int detectedArrayDepth,
                                   NamePath location, ProcessBundle<SchemaConversionError> errors) {
        data = convertDataToMatchedType(data,type.getType(),field,location,errors);

        assert detectedArrayDepth<=type.getArrayDepth();
        if (detectedArrayDepth<type.getArrayDepth()) {
            if (settings.deepenArrays()) {
                //Need to nest array to match depth
                data = deepenArray(data, type.getArrayDepth() - detectedArrayDepth);
            } else {
                errors.add(SchemaConversionError.fatal(location, "Array [%s] does not same dimension as schema [%s]", data, type));
            }
        }
        return data;
    }

}
