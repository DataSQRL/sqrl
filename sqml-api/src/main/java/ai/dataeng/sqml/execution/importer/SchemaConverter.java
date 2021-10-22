package ai.dataeng.sqml.execution.importer;

import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.ingest.schema.FlexibleSchemaHelper;
import ai.dataeng.sqml.schema2.ArrayType;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.StandardField;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.constraint.Cardinality;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.schema2.constraint.ConstraintHelper;
import ai.dataeng.sqml.schema2.constraint.NotNull;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.SpecialName;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SchemaConverter {

    public RelationType<StandardField> convert(FlexibleDatasetSchema schema) {
        RelationType.Builder<StandardField> builder = new RelationType.Builder<>();
        for (FlexibleDatasetSchema.TableField table : schema) {
            builder.add(convert(table));
        }
        return builder.build();
    }

    public StandardField convert(FlexibleDatasetSchema.TableField table) {
        return convert(table, table.getName());
    }

    public StandardField convert(FlexibleDatasetSchema.TableField table, Name newName) {
        return new StandardField(newName,new ArrayType(convert(table.getFields())),table.getConstraints());
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
            result.add(convert(field,ft, field.getTypes().size()>1));
        }
        return result;
    }

    private StandardField convert(FlexibleDatasetSchema.FlexibleField field, FlexibleDatasetSchema.FieldType ftype,
                                  final boolean isMixedType) {
        List<Constraint> constraints = ftype.getConstraints().stream()
                .filter(c -> {
                    //Since we map mixed types onto multiple fields, not-null no longer applies
                    if (c instanceof NotNull && isMixedType) return false;
                    return true;
                })
                .collect(Collectors.toList());
        return new StandardField(FlexibleSchemaHelper.getCombinedName(field,ftype),
                convert(ftype.getType(), ftype.getArrayDepth(), ftype.getConstraints()),
                constraints);
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
