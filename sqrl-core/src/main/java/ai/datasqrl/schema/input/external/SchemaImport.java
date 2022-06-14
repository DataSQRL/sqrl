package ai.datasqrl.schema.input.external;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.util.NamedIdentifier;
import ai.datasqrl.config.util.StringNamedId;
import ai.datasqrl.io.sources.dataset.DatasetRegistry;
import ai.datasqrl.io.sources.dataset.SourceDataset;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.SpecialName;
import ai.datasqrl.schema.input.RelationType;
import ai.datasqrl.schema.type.Type;
import ai.datasqrl.schema.type.basic.BasicType;
import ai.datasqrl.schema.type.basic.BasicTypeManager;
import ai.datasqrl.schema.constraint.Cardinality;
import ai.datasqrl.schema.constraint.Constraint;
import ai.datasqrl.schema.constraint.Constraint.Lookup;
import ai.datasqrl.schema.constraint.ConstraintHelper;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.SchemaElementDescription;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;
import lombok.Value;

/**
 * Converts a {@link SchemaDefinition} that is parsed out of a YAML file into a {@link
 * FlexibleDatasetSchema} to be used internally.
 * <p>
 * A {@link SchemaDefinition} is provided by a user in connection with an SQML script to specify the
 * expected schema of the source datasets consumed by the script.
 */
public class SchemaImport {

  public static final NamedIdentifier VERSION = StringNamedId.of("1");

  private final DatasetRegistry datasetLookup;
  private final Lookup constraintLookup;

  public SchemaImport(DatasetRegistry datasetLookup, Lookup constraintLookup) {
    this.datasetLookup = datasetLookup;
    this.constraintLookup = constraintLookup;
  }

  public Map<Name, FlexibleDatasetSchema> convertImportSchema(SchemaDefinition schema,
      @NonNull ErrorCollector errors) {
    NamedIdentifier version;
    if (Strings.isNullOrEmpty(schema.version)) {
      version = VERSION;
    } else {
      version = StringNamedId.of(schema.version);
    }
    if (!version.equals(VERSION)) {
      errors.fatal("Unrecognized version: %s. Supported versions are: %s", version, VERSION);
      return Collections.EMPTY_MAP;
    }
    Map<Name, FlexibleDatasetSchema> result = new HashMap<>(schema.datasets.size());
    for (DatasetDefinition dataset : schema.datasets) {
      if (Strings.isNullOrEmpty(dataset.name)) {
        errors.fatal("Missing or invalid dataset name: %s", dataset.name);
        continue;
      }
      SourceDataset sd = datasetLookup.getDataset(Name.system(dataset.name));
      if (sd == null) {
        errors.fatal("Source dataset is unknown and has not been registered with system: %s",
            dataset.name);
        continue;
      }
      if (result.containsKey(sd.getName())) {
        errors.warn(
            "Dataset [%s] is defined multiple times in schema and later definitions are ignored",
            dataset.name);
        continue;
      }
      errors = errors.resolve(sd.getName());
      FlexibleDatasetSchema ddschema = convert(dataset, sd, errors);
      result.put(sd.getName(), ddschema);
    }
    return result;
  }

  private FlexibleDatasetSchema convert(DatasetDefinition dataset, SourceDataset source,
      @NonNull ErrorCollector errors) {
    FlexibleDatasetSchema.Builder builder = new FlexibleDatasetSchema.Builder();
    builder.setDescription(SchemaElementDescription.of(dataset.description));
    for (TableDefinition table : dataset.tables) {
      Optional<FlexibleDatasetSchema.TableField> tableConvert = convert(table, source, errors);
      if (tableConvert.isPresent()) {
        builder.add(tableConvert.get());
      }
    }
    return builder.build();
  }

  private Optional<FlexibleDatasetSchema.TableField> convert(TableDefinition table,
      SourceDataset source, @NonNull ErrorCollector errors) {
    FlexibleDatasetSchema.TableField.Builder builder = new FlexibleDatasetSchema.TableField.Builder();
    Optional<Name> nameOpt = convert(table, builder, source, errors);
    if (nameOpt.isEmpty()) {
      return Optional.empty();
    } else {
      errors = errors.resolve(nameOpt.get());
    }
    builder.setPartialSchema(table.partial_schema == null ? TableDefinition.PARTIAL_SCHEMA_DEFAULT
        : table.partial_schema);
    builder.setConstraints(convertConstraints(table.tests, source, errors));
    if (table.columns == null || table.columns.isEmpty()) {
      errors.fatal("Table does not have column definitions");
      return Optional.empty();
    }
    builder.setFields(convert(table.columns, source, errors));
    return Optional.of(builder.build());
  }

  private RelationType<FlexibleDatasetSchema.FlexibleField> convert(List<FieldDefinition> columns,
      SourceDataset source, @NonNull ErrorCollector errors) {
    RelationType.Builder<FlexibleDatasetSchema.FlexibleField> rbuilder = new RelationType.Builder();
    for (FieldDefinition fd : columns) {
      Optional<FlexibleDatasetSchema.FlexibleField> fieldConvert = convert(fd, source, errors);
      if (fieldConvert.isPresent()) {
        rbuilder.add(fieldConvert.get());
      }
    }
    return rbuilder.build();
  }

  private Optional<FlexibleDatasetSchema.FlexibleField> convert(FieldDefinition field,
      SourceDataset source, @NonNull ErrorCollector errors) {
    FlexibleDatasetSchema.FlexibleField.Builder builder = new FlexibleDatasetSchema.FlexibleField.Builder();
    Optional<Name> nameOpt = convert(field, builder, source, errors);
    if (nameOpt.isEmpty()) {
      return Optional.empty();
    } else {
      errors = errors.resolve(nameOpt.get());
    }
    //Add types
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
        Optional<Name> name = convert(entry.getKey(), source, errors);
        if (name.isPresent()) {
          ftds.put(name.get(), entry.getValue());
        }
      }
    } else if (field.columns != null || field.type != null) {
      ftds = Map.of(SpecialName.SINGLETON, field);
    } else {
      ftds = Collections.EMPTY_MAP;
    }
    final List<FlexibleDatasetSchema.FieldType> types = new ArrayList<>();
    for (Map.Entry<Name, FieldTypeDefinition> entry : ftds.entrySet()) {
      Optional<FlexibleDatasetSchema.FieldType> ft = convert(entry.getKey(), entry.getValue(),
          source, errors);
      if (ft.isPresent()) {
        types.add(ft.get());
      }
    }
    builder.setTypes(types);
    return Optional.of(builder.build());
  }

  private Optional<FlexibleDatasetSchema.FieldType> convert(Name variant, FieldTypeDefinition ftd,
      SourceDataset source, @NonNull ErrorCollector errors) {
    errors = errors.resolve(variant);
    final Type type;
    final int arrayDepth;
    List<Constraint> constraints = convertConstraints(ftd.getTests(), source, errors);
    if (ftd.getColumns() != null) {
      if (ftd.getType() != null) {
        errors.warn("Cannot define columns and type. Type is ignored");
      }
      arrayDepth = ConstraintHelper.getConstraint(constraints, Cardinality.class)
          .map(c -> c.isSingleton() ? 0 : 1).orElse(1);
      type = convert(ftd.getColumns(), source, errors);
    } else if (!Strings.isNullOrEmpty(ftd.getType())) {
      BasicTypeParse btp = BasicTypeParse.parse(ftd.getType());
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
    return Optional.of(new FlexibleDatasetSchema.FieldType(variant, type, arrayDepth, constraints));
  }

  private List<Constraint> convertConstraints(List<String> tests, SourceDataset source,
      @NonNull ErrorCollector errors) {
    if (tests == null) {
      return Collections.EMPTY_LIST;
    }
    List<Constraint> constraints = new ArrayList<>(tests.size());
    for (String testString : tests) {
      Constraint.Factory cf = constraintLookup.get(testString);
      if (cf == null) {
        errors.warn("Unknown test [%s] - this constraint is ignored", testString);
        continue;
      }
      //TODO: extract parameters from yaml
      Optional<Constraint> r = cf.create(Collections.EMPTY_MAP, errors);
      if (r.isPresent()) {
        constraints.add(r.get());
      }
    }
    return constraints;
  }

  private Optional<Name> convert(String sname, SourceDataset source,
      @NonNull ErrorCollector errors) {
    if (Strings.isNullOrEmpty(sname)) {
      errors.fatal("Missing or invalid field name: %s", sname);
      return Optional.empty();
    } else {
      Name name = source.getCanonicalizer().name(sname);
      return Optional.of(name);
    }
  }

  private Optional<Name> convert(AbstractElementDefinition element,
      FlexibleDatasetSchema.AbstractField.Builder builder,
      SourceDataset source, @NonNull ErrorCollector errors) {
    final Optional<Name> name = convert(element.name, source, errors);
    if (name.isPresent()) {
      builder.setName(name.get());
      errors = errors.resolve(name.get());
    }
    builder.setDescription(SchemaElementDescription.of(element.description));
    builder.setDefault_value(
        element.default_value); //TODO: Validate that default value has right type
    return name;
  }

  @Value
  public static class BasicTypeParse {

    private final int arrayDepth;
    private final BasicType type;

    public static BasicTypeParse parse(String basicType) {
      basicType = basicType.trim();
      int depth = 0;
      while (basicType.startsWith("[") && basicType.endsWith("]")) {
        depth++;
        basicType = basicType.substring(1, basicType.length() - 1);
      }
      BasicType type = BasicTypeManager.getTypeByName(basicType);
      if (type == null) {
        return null;
      }
      return new BasicTypeParse(depth, type);
    }

    public static String export(FlexibleDatasetSchema.FieldType ft) {
      Preconditions.checkArgument(ft.getType() instanceof BasicType);
      return export(ft.getArrayDepth(), (BasicType) ft.getType());
    }

    public static String export(int arrayDepth, BasicType type) {
      String r = type.getName();
      for (int i = 0; i < arrayDepth; i++) {
        r = "[" + r + "]";
      }
      return r;
    }

  }

}
