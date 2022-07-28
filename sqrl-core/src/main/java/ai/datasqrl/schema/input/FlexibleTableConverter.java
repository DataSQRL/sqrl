package ai.datasqrl.schema.input;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.type.ArrayType;
import ai.datasqrl.schema.type.Type;
import ai.datasqrl.schema.type.basic.BasicType;
import ai.datasqrl.schema.constraint.Cardinality;
import ai.datasqrl.schema.constraint.ConstraintHelper;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Value;

import java.util.*;

@Value
@AllArgsConstructor
public class FlexibleTableConverter {

    private final InputTableSchema tableSchema;
    private final Optional<Name> tableAlias;

    public FlexibleTableConverter(InputTableSchema tableSchema) {
        this(tableSchema, Optional.empty());
    }

    public<T> Optional<T> apply(Visitor<T> visitor) {
        return visitRelation(NamePath.ROOT, tableAlias.orElse(tableSchema.getSchema().getName()), tableSchema.getSchema().getFields(),
                false, false, tableSchema.isHasSourceTimestamp(), visitor);
    }

    private<T> Optional<T> visitRelation(NamePath path, Name name, RelationType<FlexibleDatasetSchema.FlexibleField> relation,
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
        boolean notnull = !isMixedType && ConstraintHelper.isNonNull(ftype.getConstraints());

        boolean isNested = false;
        boolean isSingleton = false;
        T resultType;
        if (ftype.getType() instanceof RelationType) {
            isNested = true;
            isSingleton = isSingleton(ftype);
            Optional<T> relType = visitRelation(path, fieldName, (RelationType<FlexibleDatasetSchema.FlexibleField>) ftype.getType(), true,
                    isSingleton, false, visitor);
            Preconditions.checkArgument(relType.isPresent());
            resultType = relType.get();
            if (!isSingleton(ftype)) {
                resultType = visitor.wrapArray(resultType,true);
            }
            notnull = !isMixedType && !hasZeroOneMultiplicity(ftype);
        } else if (ftype.getType() instanceof ArrayType) {
            resultType = wrapArrayType(path.concat(fieldName), (ArrayType) ftype.getType(), visitor);
        } else {
            assert ftype.getType() instanceof BasicType;
            resultType = visitor.convertBasicType((BasicType) ftype.getType());
        }
        visitor.addField(fieldName, resultType, notnull, isNested, isSingleton);
    }

    private static<T> T wrapArrayType(NamePath path, ArrayType arrType, Visitor<T> visitor) {
        Type subType = arrType.getSubType();
        T result;
        if (subType instanceof ArrayType) {
            result = wrapArrayType(path, (ArrayType) subType, visitor);
        } else {
            assert subType instanceof BasicType;
            result = visitor.convertBasicType((BasicType) subType);
        }
        return visitor.wrapArray(result,false);
    }

    private static boolean isSingleton(FlexibleDatasetSchema.FieldType ftype) {
        return ConstraintHelper.getCardinality(ftype.getConstraints()).isSingleton();
    }

    private static boolean hasZeroOneMultiplicity(FlexibleDatasetSchema.FieldType ftype) {
        Cardinality card = ConstraintHelper.getCardinality(ftype.getConstraints());
        return card.isSingleton() && card.getMin() == 0;
    }

    public interface Visitor<T> {

        default void beginTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton,
                                boolean hasSourceTimestamp) {

        }

        Optional<T> endTable(Name name, NamePath namePath, boolean isNested, boolean isSingleton);

        void addField(Name name, T type, boolean nullable, boolean isNested, boolean isSingleTon);

        default void addField(Name name, BasicType type, boolean nullable) {
            addField(name,convertBasicType(type),nullable,false, false);
        }

        T convertBasicType(BasicType type);

        T wrapArray(T type, boolean nullable);
    }

}
