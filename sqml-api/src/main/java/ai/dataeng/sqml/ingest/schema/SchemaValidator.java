package ai.dataeng.sqml.ingest.schema;

import ai.dataeng.sqml.ingest.DatasetRegistration;
import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.ingest.stats.FieldStats;
import ai.dataeng.sqml.ingest.stats.FieldTypeStats;
import ai.dataeng.sqml.ingest.stats.SchemaGenerator;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.basic.StringType;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NameCanonicalizer;
import ai.dataeng.sqml.schema2.name.NamePath;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.function.BiPredicate;

/**
 * Follows {@link ai.dataeng.sqml.ingest.stats.SchemaGenerator} in structure and semantics.
 */
public class SchemaValidator {

    private final SchemaAdjustmentSettings settings;
    private final FlexibleDatasetSchema.TableField tableSchema;
    private final NameCanonicalizer canonicalizer;

    public SchemaValidator(@NonNull FlexibleDatasetSchema.TableField tableSchema, @NonNull SchemaAdjustmentSettings settings,
                           @NonNull DatasetRegistration dataset) {
        Preconditions.checkArgument(!tableSchema.isPartialSchema());
        this.settings = settings;
        this.tableSchema = tableSchema;
        this.canonicalizer = dataset.getCanonicalizer();
    }


    public SourceRecord<Name> verifyAndAdjust(SourceRecord<String> record, ConversionError.Bundle<SchemaConversionError> errors) {
        Map<Name,Object> result = verifyAndAdjust(record.getData(), tableSchema.getFields(), NamePath.ROOT, errors);
        return record.replaceData(result);
    }

    private Map<Name,Object> verifyAndAdjust(Map<String, Object> relationData, RelationType<FlexibleDatasetSchema.FlexibleField> relationSchema,
                                             NamePath location, ConversionError.Bundle<SchemaConversionError> errors) {
        int nonNullElements = 0;
        Map<Name,Object> result = new HashMap<>(relationData.size());
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
        }

        for (FlexibleDatasetSchema.FlexibleField field : relationSchema.getFields()) {
            if (result.get(field.getName())==null && isNonNull(field)) {
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
                                         ConversionError.Bundle<SchemaConversionError> errors) {
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

    private static BasicType inferType(Map<String, Object> originalComposite, List<FlexibleDatasetSchema.FieldType> ftypes) {
        return inferTypeInternal((t,d) -> t.conversion().isInferredType(d),originalComposite,ftypes);
    }

    private static BasicType inferType(String original, List<FlexibleDatasetSchema.FieldType> ftypes) {
        return inferTypeInternal((t,d) -> t.conversion().isInferredType(d),original,ftypes);
    }

    private static<O> BasicType inferTypeInternal(BiPredicate<BasicType, O> typeFilter, O data, List<FlexibleDatasetSchema.FieldType> ftypes) {
        return ftypes.stream().filter(t -> t.getType() instanceof BasicType).map(t -> (BasicType)t.getType())
                .filter(t -> typeFilter.test(t,data)).findFirst().orElse(null);
    }

    private Pair<Name,Object> verifyAndAdjust(Object data, FlexibleDatasetSchema.FlexibleField field,
                                              NamePath location, ConversionError.Bundle<SchemaConversionError> errors) {
        List<FlexibleDatasetSchema.FieldType> types = field.getTypes();
        FieldTypeStats.TypeSignature typeSignature = FieldStats.detectTypeSignature(data, s -> inferType(s,types),
                m -> inferType(m,types));
        FlexibleDatasetSchema.FieldType match = SchemaGenerator.matchType(typeSignature.getRaw(), typeSignature.getDetected(), types);
        if (match != null) {
            Object converted = verifyAndAdjust(data, match, field, typeSignature.getRaw().getArrayDepth(), location, errors);
            return ImmutablePair.of(FlexibleSchemaHelper.getCombinedName(field,match),converted);
        } else {
            errors.add(SchemaConversionError.notice(location, "Cannot match field data [%s] onto schema field [%s], hence field is ignored", data, field));
            return null;
        }
    }

    private Object convertDataToMatchedType(Object data, Type type, FlexibleDatasetSchema.FlexibleField field,
                                            NamePath location, ConversionError.Bundle<SchemaConversionError> errors) {
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
                                         NamePath location, ConversionError.Bundle<SchemaConversionError> errors) {
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
            data = type.conversion().parseDetected(data);
        }
        try {
            return type.conversion().convert(data);
        } catch (IllegalArgumentException e) {
            errors.add(SchemaConversionError.fatal(location,"Could not convert [%s] to [%s]", data, type));
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
                                   NamePath location, ConversionError.Bundle<SchemaConversionError> errors) {
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
