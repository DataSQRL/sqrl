package ai.dataeng.sqml.ingest.schema.external;

import ai.dataeng.sqml.ingest.DatasetLookup;
import ai.dataeng.sqml.ingest.DatasetRegistration;
import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.ingest.schema.SchemaElementDescription;
import ai.dataeng.sqml.ingest.schema.version.StringVersionId;
import ai.dataeng.sqml.ingest.schema.version.VersionIdentifier;
import ai.dataeng.sqml.ingest.source.SourceDataset;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.basic.ConversionResult;
import ai.dataeng.sqml.schema2.constraint.Cardinality;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.schema2.constraint.ConstraintHelper;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NamePath;
import ai.dataeng.sqml.schema2.name.SpecialName;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Value;

import java.util.*;

/**
 * Converts a {@link SchemaDefinition} that is parsed out of a YAML file into a {@link FlexibleDatasetSchema}
 * to be used internally.
 *
 * A {@link SchemaDefinition} is provided by a user in connection with an SQML script to specify the expected
 * schema of the source datasets consumed by the script.
 *
 */
public class SchemaImport {

    public static final VersionIdentifier VERSION = StringVersionId.of("1");

    private final DatasetLookup datasetLookup;
    private final Constraint.Lookup constraintLookup;
    private List<SchemaConversionError> errors;

    public SchemaImport(DatasetLookup datasetLookup, Constraint.Lookup constraintLookup) {
        this.datasetLookup = datasetLookup;
        this.constraintLookup = constraintLookup;
    }

    private void addError(SchemaConversionError error) {
        errors.add(error);
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    public List<SchemaConversionError> getErrors() {
        return errors;
    }

    public Map<Name, FlexibleDatasetSchema> convertImportSchema(SchemaDefinition schema) {
        errors = new ArrayList<>();
        VersionIdentifier version;
        if (Strings.isNullOrEmpty(schema.version)) version = VERSION;
        else version = StringVersionId.of(schema.version);
        if (!version.equals(VERSION)) {
            addError(SchemaConversionError.fatal(NamePath.ROOT, "Unrecognized version: %s. Supported versions are: %s", version, VERSION));
            return Collections.EMPTY_MAP;
        }
        Map<Name,FlexibleDatasetSchema> result = new HashMap<>(schema.datasets.size());
        for (DatasetDefinition dataset: schema.datasets) {
            if (Strings.isNullOrEmpty(dataset.name)) {
                addError(SchemaConversionError.fatal(NamePath.ROOT, "Missing or invalid dataset name: %s", dataset.name));
                continue;
            }
            SourceDataset sd = datasetLookup.getDataset(dataset.name);
            if (sd==null) {
                addError(SchemaConversionError.fatal(NamePath.ROOT, "Source dataset is unknown and has not been registered with system: %s", dataset.name));
                continue;
            }
            DatasetRegistration reg = sd.getRegistration();
            if (result.containsKey(reg.getName())) {
                addError(SchemaConversionError.warn(NamePath.ROOT, "Dataset [%s] is defined multiple times in schema and later definitions are ignored", dataset.name));
                continue;
            }
            NamePath location = NamePath.of(reg.getName());
            FlexibleDatasetSchema ddschema = convert(location, dataset, reg);
            result.put(reg.getName(), ddschema);
        }
        return result;
    }

    private FlexibleDatasetSchema convert(NamePath location, DatasetDefinition dataset, DatasetRegistration source) {
        FlexibleDatasetSchema.Builder builder = new FlexibleDatasetSchema.Builder();
        builder.setDescription(SchemaElementDescription.of(dataset.description));
        for (TableDefinition table : dataset.tables) {
            Optional<FlexibleDatasetSchema.TableField> tableConvert = convert(location, table, source);
            if (tableConvert.isPresent()) builder.add(tableConvert.get());
        }
        return builder.build();
    }

    private Optional<FlexibleDatasetSchema.TableField> convert(NamePath location, TableDefinition table, DatasetRegistration source) {
        FlexibleDatasetSchema.TableField.Builder builder = new FlexibleDatasetSchema.TableField.Builder();
        Optional<Name> nameOpt = convert(location,table,builder,source);
        if (nameOpt.isEmpty()) return Optional.empty();
        else {
            location = location.resolve(nameOpt.get());
        }
        builder.setPartialSchema(table.partial_schema==null?TableDefinition.PARTIAL_SCHEMA_DEFAULT:table.partial_schema);
        builder.setConstraints(convertConstraints(location,table.tests,source));
        if (table.columns==null || table.columns.isEmpty()) {
            addError(SchemaConversionError.fatal(location,"Table does not have column definitions"));
            return Optional.empty();
        }
        builder.setFields(convert(location,table.columns,source));
        return Optional.of(builder.build());
    }

    private RelationType<FlexibleDatasetSchema.FlexibleField> convert(NamePath location, List<FieldDefinition> columns, DatasetRegistration source) {
        RelationType.Builder<FlexibleDatasetSchema.FlexibleField> rbuilder = new RelationType.Builder();
        for (FieldDefinition fd : columns) {
            Optional<FlexibleDatasetSchema.FlexibleField> fieldConvert = convert(location, fd, source);
            if (fieldConvert.isPresent()) rbuilder.add(fieldConvert.get());
        }
        return rbuilder.build();
    }

    private Optional<FlexibleDatasetSchema.FlexibleField> convert(NamePath location, FieldDefinition field, DatasetRegistration source) {
        FlexibleDatasetSchema.FlexibleField.Builder builder = new FlexibleDatasetSchema.FlexibleField.Builder();
        Optional<Name> nameOpt = convert(location,field,builder,source);
        if (nameOpt.isEmpty()) return Optional.empty();
        else {
            location = location.resolve(nameOpt.get());
        }
        //Add types
        final Map<Name, FieldTypeDefinition> ftds;
        if (field.mixed!=null) {
            if (field.type!=null || field.columns!=null || field.tests!=null) {
                addError(SchemaConversionError.warn(location,"When [mixed] types are defined, field level type, column, and test definitions are ignored"));
            }
            if (field.mixed.isEmpty()) {
                addError(SchemaConversionError.fatal(location,"[mixed] type are empty"));
            }
            ftds = new HashMap<>(field.mixed.size());
            for (Map.Entry<String,FieldTypeDefinitionImpl> entry : field.mixed.entrySet()) {
                Optional<Name> name = convert(location,entry.getKey(),source);
                if (name.isPresent()) ftds.put(name.get(),entry.getValue());
            }
        } else if (field.columns!=null || field.type!=null) {
            ftds = Map.of(SpecialName.SINGLETON, field);
        } else {
            ftds = Collections.EMPTY_MAP;
        }
        final List<FlexibleDatasetSchema.FieldType> types = new ArrayList<>();
        for (Map.Entry<Name,FieldTypeDefinition> entry : ftds.entrySet()) {
            Optional<FlexibleDatasetSchema.FieldType> ft = convert(location, entry.getKey(), entry.getValue(), source);
            if (ft.isPresent()) types.add(ft.get());
        }
        builder.setTypes(types);
        return Optional.of(builder.build());
    }

    private Optional<FlexibleDatasetSchema.FieldType> convert(NamePath location, Name variant, FieldTypeDefinition ftd, DatasetRegistration source) {
        location = location.resolve(variant);
        final Type type;
        final int arrayDepth;
        List<Constraint> constraints = convertConstraints(location, ftd.getTests(), source);
        if (ftd.getColumns()!=null) {
            if (ftd.getType()!=null) {
                addError(SchemaConversionError.warn(location,"Cannot define columns and type. Type is ignored"));
            }
            arrayDepth = ConstraintHelper.getConstraint(constraints, Cardinality.class)
                    .map(c -> c.isSingleton()?0:1).orElse(1);
            type = convert(location,ftd.getColumns(),source);
        } else if (!Strings.isNullOrEmpty(ftd.getType())) {
            BasicTypeParse btp = BasicTypeParse.parse(ftd.getType());
            if (btp==null) {
                addError(SchemaConversionError.fatal(location,"Type unrecognized: %s", ftd.getType()));
                return Optional.empty();
            }
            type = btp.type; arrayDepth = btp.arrayDepth;
        } else {
            addError(SchemaConversionError.fatal(location,"Type definition missing (specify either [type] or [columns])"));
            return Optional.empty();
        }
        return Optional.of(new FlexibleDatasetSchema.FieldType(variant, type, arrayDepth, constraints));
    }

    private List<Constraint> convertConstraints(NamePath location, List<String> tests, DatasetRegistration source) {
        if (tests==null) return Collections.EMPTY_LIST;
        List<Constraint> constraints = new ArrayList<>(tests.size());
        for (String testString : tests) {
            Constraint.Factory cf = constraintLookup.get(testString);
            if (cf == null) {
                addError(SchemaConversionError.warn(location,"Unknown test [%s] - this constraint is ignored", testString));
                continue;
            }
            //TODO: extract parameters from yaml
            ConversionResult<Constraint, ConversionError> r = cf.create(Collections.EMPTY_MAP);
            if (r.hasError()) addError(SchemaConversionError.convert(location,r.getError()));
            if (r.hasResult()) constraints.add(r.getResult());
        }
        return constraints;
    }

    private Optional<Name> convert(NamePath location, String sname, DatasetRegistration source) {
        if (Strings.isNullOrEmpty(sname)) {
            addError(SchemaConversionError.fatal(location, "Missing or invalid field name: %s", sname));
            return Optional.empty();
        } else {
            Name name = source.toName(sname);
            return Optional.of(name);
        }
    }

    private Optional<Name> convert(NamePath location, AbstractElementDefinition element, FlexibleDatasetSchema.AbstractField.Builder builder,
            DatasetRegistration source) {
        final Optional<Name> name = convert(location, element.name, source);
        if (name.isPresent()) {
            builder.setName(name.get());
            location = location.resolve(name.get());
        }
        builder.setDescription(SchemaElementDescription.of(element.description));
        builder.setDefault_value(element.default_value); //TODO: Validate that default value has right type
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
                basicType = basicType.substring(1,basicType.length()-1);
            }
            BasicType type = BasicType.getTypeByName(basicType);
            if (type==null) return null;
            return new BasicTypeParse(depth,type);
        }

        public static String export(FlexibleDatasetSchema.FieldType ft) {
            Preconditions.checkArgument(ft.getType() instanceof BasicType);
            return export(ft.getArrayDepth(),(BasicType) ft.getType());
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
