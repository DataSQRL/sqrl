/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.input;

import com.datasqrl.engine.stream.FunctionWithError;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.stats.DefaultSchemaGenerator;
import com.datasqrl.io.stats.FieldStats;
import com.datasqrl.io.stats.SchemaGenerator;
import com.datasqrl.io.stats.TypeSignature.Simple;
import com.datasqrl.name.Name;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.schema.type.Type;
import com.datasqrl.schema.type.basic.BasicType;
import com.datasqrl.schema.type.basic.StringType;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

/**
 * Follows {@link DefaultSchemaGenerator} in structure and semantics.
 */
public class DefaultSchemaValidator implements SchemaValidator, Serializable {

  private final SchemaAdjustmentSettings settings;
  private final InputTableSchema<FlexibleTableSchema> tableSchema;
  private final NameCanonicalizer canonicalizer;
  private final SchemaGenerator schemaGenerator;

  public DefaultSchemaValidator(@NonNull InputTableSchema<FlexibleTableSchema> tableSchema,
      @NonNull SchemaAdjustmentSettings settings,
      @NonNull NameCanonicalizer canonicalizer,
      @NonNull SchemaGenerator schemaGenerator) {
    Preconditions.checkArgument(!tableSchema.getSchema().isPartialSchema());
    this.settings = settings;
    this.tableSchema = tableSchema;
    this.canonicalizer = canonicalizer;
    this.schemaGenerator = schemaGenerator;
  }

  public FunctionWithError<SourceRecord.Raw, SourceRecord.Named> getFunction() {
    return new Function(this);
  }

  private SourceRecord.Named verifyAndAdjust(SourceRecord.Raw record, ErrorCollector errors) {
    //verify meta data
    if (!record.hasUUID()) {
      errors.fatal("Input record does not have UUID");
    }
    if (record.getIngestTime() == null) {
      errors.fatal("Input record does not ingest timestamp");
    }
    if (tableSchema.isHasSourceTimestamp() && record.getSourceTime() == null) {
      errors.fatal("Input record does not source timestamp");
    }
    Map<Name, Object> result = verifyAndAdjust(record.getData(),
        tableSchema.getSchema().getFields(), errors);
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
          errors.fatal("Field is not defined in schema: %s", field);
        }
      } else {
        Pair<Name, Object> fieldResult = null;
        if (data != null) {
          fieldResult = verifyAndAdjust(data, field.get(), errors.resolve(name));
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
    errors.fatal("Field [%s] has non-null constraint but record contains null value", field);
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
    Simple typeSignature = FieldStats.detectTypeSignature(data, s -> detectType(s, types),
        m -> detectType(m, types));
    FlexibleFieldSchema.FieldType match = schemaGenerator.matchType(typeSignature, types);
    if (match != null) {
      Object converted = verifyAndAdjust(data, match, field, typeSignature.getArrayDepth(), errors);
      return ImmutablePair.of(FlexibleSchemaHelper.getCombinedName(field, match), converted);
    } else {
      errors.notice("Cannot match field data [%s] onto schema field [%s], hence field is ignored",
          data, field);
      return null;
    }
  }

  private Object convertDataToMatchedType(Object data, Type type,
      FlexibleFieldSchema.Field field,
      ErrorCollector errors) {
    if (FieldStats.isArray(data)) {
      Collection<Object> col = FieldStats.array2Collection(data);
      List<Object> result = new ArrayList<>(col.size());
      for (Object o : col) {
        if (o == null) {
          if (!settings.removeListNulls()) {
            errors.fatal("Array contains null values: [%s]", col);
          }
        } else {
          result.add(convertDataToMatchedType(o, type, field, errors));
        }
      }
      return result;
    } else {
      if (type instanceof RelationType) {
        if (data instanceof Map) {
          return verifyAndAdjust((Map) data, (RelationType) type, errors);
        } else {
          errors.fatal("Expected composite object [%s]", data);
          return null;
        }
      } else {
        assert type instanceof BasicType;
        return cast2BasicType(data, (BasicType) type, errors);
      }
    }
  }

  private Object cast2BasicType(Object data, BasicType type, ErrorCollector errors) {
    if (type instanceof StringType) {
      if (data instanceof String) {
        return data;
      } else { //Cast to string
        if (!settings.castDataType()) {
          errors.fatal("Expected string but got: %s", data);
        }
        return data.toString();
      }
    }
    if (data instanceof String || data instanceof Map) {
      //The datatype was detected
      if (!settings.castDataType()) {
        errors.fatal("Encountered [%s] but expected [%s]", data, type);
      }
      Optional<Object> result = type.conversion().parseDetected(data, errors);
      if (result.isEmpty()) {
        errors.fatal("Could not parse [%s] for type [%s]", data, type);
        return null;
      } else {
        data = result.get();
      }
    }
    try {
      return type.conversion().convert(data);
    } catch (IllegalArgumentException e) {
      errors.fatal("Could not convert [%s] to type [%s]", data, type);
      return null;
    }
  }

  private Object deepenArray(Object data, int additionalDepth) {
    Preconditions.checkNotNull(data);
    Preconditions.checkNotNull(additionalDepth >= 0);
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
        errors.fatal("Array [%s] does not same dimension as schema [%s]", data, type);
      }
    }
    return data;
  }

  @AllArgsConstructor
  public static class Function implements FunctionWithError<SourceRecord.Raw, SourceRecord.Named> {

    private final DefaultSchemaValidator validator;

    @Override
    public Optional<SourceRecord.Named> apply(SourceRecord.Raw raw,
        Supplier<ErrorCollector> errorCollectorSupplier) {
      ErrorCollector errors = errorCollectorSupplier.get();
      SourceRecord.Named result = validator.verifyAndAdjust(raw, errors);
      if (errors.isFatal()) {
        return Optional.empty();
      } else {
        return Optional.of(result);
      }
    }
  }

}
