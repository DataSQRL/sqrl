/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.input;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.tables.SchemaValidator;
import com.datasqrl.schema.input.FlexibleFieldSchema.Field;
import com.datasqrl.schema.input.TypeSignature.Simple;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.schema.type.Type;
import com.datasqrl.schema.type.basic.BasicType;
import com.datasqrl.schema.type.basic.StringType;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiPredicate;

/**
 * Validates raw input data against the flexible table schema and converts it to named data
 */
public class FlexibleSchemaValidator implements SchemaValidator, Serializable {

  private final SchemaAdjustmentSettings settings;
  private final FlexibleTableSchema tableSchema;
  private final boolean hasSourceTimestamp;

  private final NameCanonicalizer canonicalizer;
  private final FlexibleTypeMatcher typeMatcher;

  public FlexibleSchemaValidator(@NonNull FlexibleTableSchema tableSchema,
                                 boolean hasSourceTimestamp,
                                 @NonNull SchemaAdjustmentSettings settings,
                                 @NonNull NameCanonicalizer canonicalizer,
                                 @NonNull FlexibleTypeMatcher typeMatcher) {
//    Preconditions.checkArgument(!tableSchema.isPartialSchema());
    this.settings = settings;
    this.tableSchema = tableSchema;
    this.hasSourceTimestamp = hasSourceTimestamp;
    this.canonicalizer = canonicalizer;
    this.typeMatcher = typeMatcher;
  }

  public SourceRecord.Named verifyAndAdjust(SourceRecord.Raw record, ErrorCollector errors) {
    //verify meta data
    errors.checkFatal(record.hasUUID(),"Input record does not have UUID: %s [table=%s]", record, tableSchema.getName());
    errors.checkFatal(record.getIngestTime()!=null, "Input record does not ingest timestamp: %s [table=%s]", record, tableSchema.getName());
    errors.checkFatal(!hasSourceTimestamp || record.getSourceTime()!=null, "Input record does not have source timestamp: %s [table=%s]", record, tableSchema.getName());
    Map<Name, Object> result = verifyAndAdjust(record.getData(),
        tableSchema.getFields(), errors);
    return record.replaceData(result);
  }

  private Map<Name, Object> verifyAndAdjust(Map<String, Object> relationData,
      RelationType<FlexibleFieldSchema.Field> relationSchema,
      ErrorCollector errors) {
    Map<Name, Object> result = new LinkedHashMap<>(relationData.size());
    Set<Name> visitedFields = new HashSet<>();
    for (Map.Entry<String, Object> entry : relationData.entrySet()) {
      Name name = Name.of(entry.getKey(), canonicalizer);
      Object data = entry.getValue();
      Optional<FlexibleFieldSchema.Field> field = relationSchema.getFieldByName(name);
      if (field.isEmpty()) {
        if (!settings.dropFields()) {
          errors.fatal("Field is not defined in schema: %s [table=%s]", name, tableSchema.getName());
        }
      } else {
        Pair<Name, Object> fieldResult = null;
        if (data != null) {
          fieldResult = verifyAndAdjust(data, field.get(), errors.resolve(name.getDisplay()));
        }
        if (fieldResult == null && isNonNull(field.get())) {
          fieldResult = handleNull(field.get(), errors);
        }
        if (fieldResult != null) {
          result.put(fieldResult.getKey(), fieldResult.getValue());
        }
      }
      visitedFields.add(name);
    }

    //See if we missed any non-null fields
    for (FlexibleFieldSchema.Field field : relationSchema.getFields()) {
      if (!visitedFields.contains(field.getName()) && isNonNull(field)) {
        Pair<Name, Object> fieldResult = handleNull(field, errors);
        if (fieldResult != null) {
          result.put(fieldResult.getKey(), fieldResult.getValue());
        }
      }
    }
    return result;
  }

  private boolean isNonNull(FlexibleFieldSchema.Field field) {
    //Use memoization to reduce repeated computation
    return FlexibleSchemaHelper.isNonNull(field);
  }

  private Pair<Name, Object> handleNull(FlexibleFieldSchema.Field field,
                                        ErrorCollector errors) {
    //See if we can map this onto any field type
    for (FlexibleFieldSchema.FieldType ft : field.getTypes()) {
      if (ft.getArrayDepth() > 0 && settings.null2EmptyArray()) {
        return ImmutablePair.of(FlexibleSchemaHelper.getCombinedName(field, ft),
            deepenArray(Collections.EMPTY_LIST, ft.getArrayDepth() - 1));
      }
    }
    errors.fatal("Field [%s] has non-null constraint but record contains null value [table=%s]", field.getName(), tableSchema.getName());
    return null;
  }

  private static BasicType detectType(Map<String, Object> originalComposite,
      List<FlexibleFieldSchema.FieldType> ftypes) {
    return detectTypeInternal((t, d) -> t.conversion().detectType(d), originalComposite, ftypes);
  }

  private static BasicType detectType(String original,
      List<FlexibleFieldSchema.FieldType> ftypes) {
    return detectTypeInternal((t, d) -> t.conversion().detectType(d), original, ftypes);
  }

  private static <O> BasicType detectTypeInternal(BiPredicate<BasicType, O> typeFilter, O data,
      List<FlexibleFieldSchema.FieldType> ftypes) {
    return ftypes.stream().filter(t -> t.getType() instanceof BasicType)
        .map(t -> (BasicType) t.getType())
        .filter(t -> typeFilter.test(t, data)).findFirst().orElse(null);
  }

  private Pair<Name, Object> verifyAndAdjust(Object data, FlexibleFieldSchema.Field field,
      ErrorCollector errors) {
    List<FlexibleFieldSchema.FieldType> types = field.getTypes();
    Optional<Simple> typeSignatureOpt = TypeSignatureUtil.detectSimpleTypeSignature(data, s -> detectType(s, types),
        m -> detectType(m, types));
    if (typeSignatureOpt.isEmpty()) return null;
    Simple typeSignature = typeSignatureOpt.get();
    FlexibleFieldSchema.FieldType match = typeMatcher.matchType(typeSignature, types);
    if (match != null) {
      Object converted = verifyAndAdjust(data, match, field, typeSignature.getArrayDepth(), errors);
      return ImmutablePair.of(FlexibleSchemaHelper.getCombinedName(field, match), converted);
    } else {
      errors.notice("Cannot match field data [%s] onto schema field [%s], hence field is ignored [table=%s]",
          data, field.getName(), tableSchema.getName());
      return null;
    }
  }

  private Object convertDataToMatchedType(Object data, Type type,
      FlexibleFieldSchema.Field field,
      ErrorCollector errors) {
    if (TypeSignatureUtil.isArray(data)) {
      Collection<Object> col = TypeSignatureUtil.array2Collection(data);
      List<Object> result = new ArrayList<>(col.size());
      for (Object o : col) {
        if (o == null) {
          if (!settings.removeListNulls()) {
            errors.fatal("Array contains null values: [%s] [field=%s, table=%s]", col, field.getName(), tableSchema);
          }
        } else {
          result.add(convertDataToMatchedType(o, type, field, errors));
        }
      }
      return result;
    } else {
      if (type instanceof RelationType) {
        Optional<Field> singletonField;
        if (data instanceof Map) {
          return verifyAndAdjust((Map) data, (RelationType) type, errors);
        } else if ((singletonField=FlexibleTypeMatcher.getSingletonBasicField(
            (RelationType<Field>) type)).isPresent()) { //Singleton nested field
          Field nestedField = singletonField.get();
          FlexibleFieldSchema.FieldType nestedFieldType = nestedField.getTypes().get(0); //there's only one
          Map<Name, Object> result = new HashMap<>(1);
          result.put(FlexibleSchemaHelper.getCombinedName(nestedField, nestedFieldType),
              verifyAndAdjust(data, nestedFieldType, nestedField, 0, errors));
          return result;
        } else {
          errors.fatal("Expected composite object [%s] [field=%s, table=%s]", data, field.getName(), tableSchema.getName());
          return null;
        }
      } else {
        assert type instanceof BasicType;
        return cast2BasicType(data, (BasicType) type, field, errors);
      }
    }
  }

  private Object cast2BasicType(Object data, BasicType type, FlexibleFieldSchema.Field field,
      ErrorCollector errors) {
    if (type instanceof StringType) {
      if (data instanceof String) {
        return data;
      } else { //Cast to string
        if (!settings.castDataType()) {
          errors.fatal("Expected string but got: %s [field=%s, table=%s]", data, field.getName(), tableSchema.getName());
        }
        return data.toString();
      }
    }
    if (data instanceof String || data instanceof Map) {
      //The datatype was detected
      if (!settings.castDataType()) {
        errors.fatal("Encountered [%s] but expected [%s] [field=%s, table=%s]", data, type, field.getName(), tableSchema.getName());
      }
      Optional<Object> result = type.conversion().parseDetected(data, errors);
      if (result.isEmpty()) {
        errors.fatal("Could not parse [%s] for type [%s] [field=%s, table=%s]", data, type, field.getName(), tableSchema.getName());
        return null;
      } else {
        data = result.get();
      }
    }
    try {
      return type.conversion().convert(data);
    } catch (IllegalArgumentException e) {
      errors.fatal("Could not convert [%s] to type [%s] [field=%s, table=%s]", data, type, field.getName(), tableSchema.getName());
      return null;
    }
  }

  private Object deepenArray(Object data, int additionalDepth) {
//    Preconditions.checkNotNull(data);
//    Preconditions.checkNotNull(additionalDepth >= 0);
    for (int i = 0; i < additionalDepth; i++) {
      data = Collections.singletonList(data);
    }
    return data;
  }

  private Object verifyAndAdjust(Object data, FlexibleFieldSchema.FieldType type,
      FlexibleFieldSchema.Field field,
      int detectedArrayDepth, ErrorCollector errors) {
    data = convertDataToMatchedType(data, type.getType(), field, errors);

    assert detectedArrayDepth <= type.getArrayDepth();
    if (detectedArrayDepth < type.getArrayDepth()) {
      if (settings.deepenArrays()) {
        //Need to nest array to match depth
        data = deepenArray(data, type.getArrayDepth() - detectedArrayDepth);
      } else {
        errors.fatal("Array [%s] does not have same dimension as schema [%s] [field=%s, table=%s]",
            data, type, field.getName(), tableSchema.getName());
      }
    }
    return data;
  }
}
