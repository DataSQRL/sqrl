package ai.datasqrl.schema.input;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.constraint.Cardinality;
import ai.datasqrl.schema.constraint.ConstraintHelper;
import ai.datasqrl.schema.type.Type;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.util.Optional;

@Value
@AllArgsConstructor
public class FlexibleTableConverter {

    private final InputTableSchema tableSchema;
    private final Optional<Name> tableAlias;

    public FlexibleTableConverter(InputTableSchema tableSchema) {
        this(tableSchema, Optional.empty());
    }

    public<T> T apply(Visitor<T> visitor) {
        return visitRelation(NamePath.ROOT, tableAlias.orElse(tableSchema.getSchema().getName()), tableSchema.getSchema().getFields(),
                false, false, tableSchema.isHasSourceTimestamp(), visitor);
    }

    private<T> T visitRelation(NamePath path, Name name, RelationType<FlexibleDatasetSchema.FlexibleField> relation,
                                         boolean isNested, boolean isSingleton, boolean hasSourceTime, Visitor<T> visitor) {
        visitor.beginTable(name, path, isNested, isSingleton, hasSourceTime);
        path = path.concat(name);

        for (FlexibleDatasetSchema.FlexibleField field : relation.getFields()) {
            for (FlexibleDatasetSchema.FieldType ftype : field.getTypes()) {
                Name fieldName = FlexibleSchemaHelper.getCombinedName(field, ftype);
                boolean isMixedType = field.getTypes().size() > 1;
                visitFieldType(path, fieldName, ftype, isMixedType, visitor);
            }
        }
        return visitor.endTable(name, path, isNested, isSingleton);
    }

    private<T> void visitFieldType(NamePath path, Name fieldName, FlexibleDatasetSchema.FieldType ftype,
                                boolean isMixedType, Visitor<T> visitor) {
        boolean nullable = isMixedType || !ConstraintHelper.isNonNull(ftype.getConstraints());
        boolean isSingleton = false;
        if (ftype.getType() instanceof RelationType) {
            isSingleton = isSingleton(ftype);
            T nestedTable = visitRelation(path, fieldName, (RelationType<FlexibleDatasetSchema.FlexibleField>) ftype.getType(), true,
                    isSingleton, false, visitor);
            nullable = isMixedType || hasZeroOneMultiplicity(ftype);
            visitor.addField(fieldName, nestedTable, nullable, isSingleton);
        } else {
            visitor.addField(fieldName, ftype.getType(), nullable);
        }
    }

    private static boolean isSingleton(FlexibleDatasetSchema.FieldType ftype) {
        return ConstraintHelper.getCardinality(ftype.getConstraints()).isSingleton();
    }

    private static boolean hasZeroOneMultiplicity(FlexibleDatasetSchema.FieldType ftype) {
        Cardinality card = ConstraintHelper.getCardinality(ftype.getConstraints());
        return card.isSingleton() && card.getMin() == 0;
    }

    public interface Visitor<T> {

        void beginTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton,
                                boolean hasSourceTimestamp);

        T endTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton);

        void addField(Name name, Type type, boolean nullable);

        void addField(Name name, T nestedTable, boolean nullable, boolean isSingleTon);

//        default void addField(Name name, BasicType type, boolean nullable) {
//            addField(name,convertBasicType(type),nullable,false, false);
//        }

//        T nullable(T type, boolean nullable);
//
//        T convertBasicType(BasicType type);
//
//        T wrapArray(T type);
    }

}
