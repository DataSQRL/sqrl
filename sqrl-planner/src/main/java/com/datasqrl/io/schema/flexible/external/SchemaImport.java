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
package com.datasqrl.io.schema.flexible.external;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.SpecialName;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.constraint.Cardinality;
import com.datasqrl.io.schema.flexible.constraint.Constraint;
import com.datasqrl.io.schema.flexible.constraint.Constraint.Lookup;
import com.datasqrl.io.schema.flexible.constraint.ConstraintHelper;
import com.datasqrl.io.schema.flexible.input.FlexibleFieldSchema;
import com.datasqrl.io.schema.flexible.input.FlexibleTableSchema;
import com.datasqrl.io.schema.flexible.input.RelationType;
import com.datasqrl.io.schema.flexible.input.SchemaElementDescription;
import com.datasqrl.io.schema.flexible.input.external.AbstractElementDefinition;
import com.datasqrl.io.schema.flexible.input.external.FieldDefinition;
import com.datasqrl.io.schema.flexible.input.external.FieldTypeDefinition;
import com.datasqrl.io.schema.flexible.input.external.FieldTypeDefinitionImpl;
import com.datasqrl.io.schema.flexible.input.external.TableDefinition;
import com.datasqrl.io.schema.flexible.type.Type;
import com.datasqrl.io.schema.flexible.type.basic.BasicType;
import com.datasqrl.io.schema.flexible.type.basic.BasicTypeManager;
import com.datasqrl.util.NamedIdentifier;
import com.datasqrl.util.StringNamedId;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;

/**
 * Converts a {@link SchemaDefinition} that is parsed out of a YAML file into a {@link
 * FlexibleTableSchema} to be used internally.
 *
 * <p>A {@link SchemaDefinition} is provided by a user in connection with a table configuration to
 * specify the expected schema of the source datasets consumed by the script.
 */
public class SchemaImport {

  public static final NamedIdentifier VERSION = StringNamedId.of("1");

  private final Lookup constraintLookup;
  private final NameCanonicalizer canonicalizer;

  public SchemaImport(Lookup constraintLookup, NameCanonicalizer defaultCanonicalizer) {
    this.constraintLookup = constraintLookup;
    this.canonicalizer = defaultCanonicalizer;
  }

  public Optional<FlexibleTableSchema> convert(
      TableDefinition table, @NonNull ErrorCollector errors) {
    NamedIdentifier version;
    if (Strings.isNullOrEmpty(table.schema_version)) {
      version = VERSION;
    } else {
      version = StringNamedId.of(table.schema_version);
    }
    if (!version.equals(VERSION)) {
      errors.fatal("Unrecognized schema version: %s. Supported versions are: %s", version, VERSION);
      return Optional.empty();
    }
    var builder = new FlexibleTableSchema.Builder();
    var nameOpt = convert(table, builder, errors);
    if (nameOpt.isEmpty()) {
      return Optional.empty();
    } else {
      errors = errors.resolve(nameOpt.get().getDisplay());
    }
    builder.setPartialSchema(
        table.partial_schema == null
            ? TableDefinition.PARTIAL_SCHEMA_DEFAULT
            : table.partial_schema);
    builder.setConstraints(convertConstraints(table.tests, errors));
    if (table.columns == null || table.columns.isEmpty()) {
      errors.fatal("Table does not have column definitions");
      return Optional.empty();
    }
    builder.setFields(convert(table.columns, errors));
    return Optional.of(builder.build());
  }

  private RelationType<FlexibleFieldSchema.Field> convert(
      List<FieldDefinition> columns, @NonNull ErrorCollector errors) {
    RelationType.Builder<FlexibleFieldSchema.Field> rbuilder = new RelationType.Builder();
    for (FieldDefinition fd : columns) {
      var fieldConvert = convert(fd, errors);
      if (fieldConvert.isPresent()) {
        rbuilder.add(fieldConvert.get());
      }
    }
    return rbuilder.build();
  }

  private Optional<FlexibleFieldSchema.Field> convert(
      FieldDefinition field, @NonNull ErrorCollector errors) {
    var builder = new FlexibleFieldSchema.Field.Builder();
    var nameOpt = convert(field, builder, errors);
    if (nameOpt.isEmpty()) {
      return Optional.empty();
    } else {
      errors = errors.resolve(nameOpt.get().getDisplay());
    }
    // Add types
    final Map<Name, FieldTypeDefinition> ftds;
    if (field.mixed != null) {
      if (field.type != null || field.columns != null || field.tests != null) {
        errors.warn(
            "When [mixed] types are defined, field level type, column, and test definitions are ignored");
      }
      if (field.mixed.isEmpty()) {
        errors.fatal("[mixed] type are empty");
      }
      ftds = new HashMap<>(field.mixed.size());
      for (Map.Entry<String, FieldTypeDefinitionImpl> entry : field.mixed.entrySet()) {
        var name = convert(entry.getKey(), errors);
        if (name.isPresent()) {
          ftds.put(name.get(), entry.getValue());
        }
      }
    } else if (field.columns != null || field.type != null) {
      ftds = Map.of(SpecialName.SINGLETON, field);
    } else {
      ftds = Collections.EMPTY_MAP;
    }
    final List<FlexibleFieldSchema.FieldType> types = new ArrayList<>();
    for (Map.Entry<Name, FieldTypeDefinition> entry : ftds.entrySet()) {
      var ft = convert(entry.getKey(), entry.getValue(), errors);
      if (ft.isPresent()) {
        types.add(ft.get());
      }
    }
    builder.setTypes(types);
    return Optional.of(builder.build());
  }

  private Optional<FlexibleFieldSchema.FieldType> convert(
      Name variant, FieldTypeDefinition ftd, @NonNull ErrorCollector errors) {
    errors = errors.resolve(variant.getDisplay());
    final Type type;
    final int arrayDepth;
    var constraints = convertConstraints(ftd.getTests(), errors);
    if (ftd.getCardinality() != null) {
      var cf = constraintLookup.get("cardinality");

      var r = cf.create(ftd.getCardinality(), errors);
      if (r.isPresent()) {
        constraints.add(r.get());
      }
    }
    if (ftd.getColumns() != null) {
      if (ftd.getType() != null) {
        errors.warn("Cannot define columns and type. Type is ignored");
      }
      arrayDepth =
          ConstraintHelper.getConstraint(constraints, Cardinality.class)
              .map(c -> c.isSingleton() ? 0 : 1)
              .orElse(1);
      type = convert(ftd.getColumns(), errors);
    } else if (!Strings.isNullOrEmpty(ftd.getType())) {
      var btp = BasicTypeParse.parse(ftd.getType());
      if (btp == null) {
        errors.fatal("Type unrecognized: %s", ftd.getType());
        return Optional.empty();
      }
      type = btp.type;
      arrayDepth = btp.arrayDepth;
    } else {
      errors.fatal("Type definition missing (specify either [type] or [columns])");
      return Optional.empty();
    }
    return Optional.of(new FlexibleFieldSchema.FieldType(variant, type, arrayDepth, constraints));
  }

  private List<Constraint> convertConstraints(List<String> tests, @NonNull ErrorCollector errors) {
    if (tests == null) {
      return Collections.EMPTY_LIST;
    }
    List<Constraint> constraints = new ArrayList<>(tests.size());
    for (String testString : tests) {
      var cf = constraintLookup.get(testString);
      if (cf == null) {
        errors.warn("Unknown test [%s] - this constraint is ignored", testString);
        continue;
      }
      // TODO: extract parameters from yaml
      Optional<Constraint> r = cf.create(Collections.EMPTY_MAP, errors);
      if (r.isPresent()) {
        constraints.add(r.get());
      }
    }
    return constraints;
  }

  private Optional<Name> convert(String sname, @NonNull ErrorCollector errors) {
    if (Strings.isNullOrEmpty(sname)) {
      errors.fatal("Missing or invalid field name: %s", sname);
      return Optional.empty();
    } else {
      var name = canonicalizer.name(sname);
      return Optional.of(name);
    }
  }

  private Optional<Name> convert(
      AbstractElementDefinition element,
      FlexibleFieldSchema.Builder builder,
      @NonNull ErrorCollector errors) {
    final var name = convert(element.name, errors);
    if (name.isPresent()) {
      builder.setName(name.get());
      errors = errors.resolve(name.get().getDisplay());
    }
    builder.setDescription(SchemaElementDescription.of(element.description));
    builder.setDefault_value(
        element.default_value); // TODO: Validate that default value has right type
    return name;
  }

  public record BasicTypeParse(int arrayDepth, BasicType type) {

    public static BasicTypeParse parse(String basicType) {
      basicType = basicType.trim();
      var depth = 0;
      while (basicType.startsWith("[") && basicType.endsWith("]")) {
        depth++;
        basicType = basicType.substring(1, basicType.length() - 1);
      }
      BasicType<?> type = BasicTypeManager.getTypeByName(basicType);
      if (type == null) {
        return null;
      }
      return new BasicTypeParse(depth, type);
    }

    public static String export(FlexibleFieldSchema.FieldType ft) {
      Preconditions.checkArgument(ft.getType() instanceof BasicType);
      return export(ft.getArrayDepth(), (BasicType<?>) ft.getType());
    }

    public static String export(int arrayDepth, BasicType<?> type) {
      var r = type.getName();
      for (var i = 0; i < arrayDepth; i++) {
        r = "[" + r + "]";
      }
      return r;
    }
  }
}
