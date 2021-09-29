package ai.dataeng.sqml.execution.importer;

import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.schema2.ArrayType;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.StandardField;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.constraint.Cardinality;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.schema2.constraint.ConstraintHelper;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.SpecialName;

import java.util.ArrayList;
import java.util.List;

public class SchemaConverter {

    public RelationType<StandardField> convert(FlexibleDatasetSchema schema) {
        RelationType.Builder<StandardField> builder = new RelationType.Builder<>();
        for (FlexibleDatasetSchema.TableField table : schema) {
            builder.add(convert(table));
        }
        return builder.build();
    }

    public StandardField convert(FlexibleDatasetSchema.TableField table) {
        return new StandardField(table.getName(),convert(table.getFields()),table.getConstraints());
    }

    public StandardField convert(FlexibleDatasetSchema.TableField table, Name newName) {
        return new StandardField(newName,convert(table.getFields()),table.getConstraints());
    }

    private RelationType<StandardField> convert(RelationType<FlexibleDatasetSchema.FlexibleField> relation) {
        RelationType.Builder<StandardField> builder = new RelationType.Builder<>();
        for (FlexibleDatasetSchema.FlexibleField field : relation) {
            for (StandardField f : convert(field)) builder.add(f);
        }
        return builder.build();
    }

    private List<StandardField> convert(FlexibleDatasetSchema.FlexibleField field) {
        List<StandardField> result = new ArrayList<>(field.getTypes().size());
        for (FlexibleDatasetSchema.FieldType ft : field.getTypes()) {
            result.add(convert(field,ft));
        }
        return result;
    }

    private StandardField convert(FlexibleDatasetSchema.FlexibleField field, FlexibleDatasetSchema.FieldType ftype) {
        Name name = field.getName();
        if (name instanceof SpecialName) {
            if (name.equals(SpecialName.VALUE)) {
                name = Name.system("_value"); //TODO: Need to check if this clashes with other names in RelationType
            } else throw new IllegalArgumentException(String.format("Unrecognized name: %s",name));
        }

        if (!ftype.getVariantName().equals(SpecialName.SINGLETON)) {
            name = Name.concatenate(field.getName(),ftype.getVariantName());
        }

        return new StandardField(name,
                convert(ftype.getType(), ftype.getArrayDepth(), ftype.getConstraints()),
                ftype.getConstraints());
    }

    private Type convert(Type type, int arrayDepth, List<Constraint> constraints) {
        Type result;
        if (type instanceof RelationType) {
            assert arrayDepth==1;
            RelationType<StandardField> relType = convert((RelationType<FlexibleDatasetSchema.FlexibleField>) type);
            if (ConstraintHelper.getConstraint(constraints, Cardinality.class).orElse(Cardinality.UNCONSTRAINED).isSingleton()) {
                result = relType;
            } else {
                result = new ArrayType(relType);
            }
        } else {
            assert type instanceof BasicType;
            result = (BasicType)type;
            for (int i = 0; i < arrayDepth; i++) {
                result = new ArrayType(result);
            }
        }
        return result;
    }

}
