package ai.datasqrl.plan.calcite.util;

import ai.datasqrl.parse.tree.name.Name;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.ArraySqlType;

import java.util.Optional;

public class CalciteUtil {

    public static boolean isNestedTable(RelDataType type) {
        if (type.isStruct()) return true;
        return getArrayElementType(type).map(RelDataType::isStruct).orElse(false);
    }

    public static Optional<RelDataType> getArrayElementType(RelDataType type) {
        if (isArray(type)) {
            return Optional.of(type.getComponentType());
        } else return Optional.empty();
    }

    public static boolean hasNesting(RelDataType type) {
        Preconditions.checkState(type.getFieldCount()>0);
        return type.getFieldList().stream().map(t -> t.getType()).anyMatch(CalciteUtil::isNestedTable);
    }

    public static boolean isArray(RelDataType type) {
        return type instanceof ArraySqlType;
    }

    public interface RelDataTypeBuilder {

        public default RelDataTypeBuilder add(Name name, RelDataType type) {
            return add(name.getCanonical(),type);
        }

        public RelDataTypeBuilder add(String name, RelDataType type);

        public default RelDataTypeBuilder add(Name name, RelDataType type, boolean nullable) {
            return add(name.getCanonical(),type,nullable);
        }

        public RelDataTypeBuilder add(String name, RelDataType type, boolean nullable);

        public RelDataTypeBuilder add(RelDataTypeField field);

        public default RelDataTypeBuilder addAll(Iterable<RelDataTypeField> fields) {
            for (RelDataTypeField field : fields) {
                add(field);
            }
            return this;
        }

        public RelDataType build();

    }

    public static RelDataTypeBuilder getRelTypeBuilder(@NonNull RelDataTypeFactory factory) {
        return new RelDataTypeFieldBuilder(factory.builder().kind(StructKind.FULLY_QUALIFIED));
    }

    public static RelDataType appendField(@NonNull RelDataType relation, @NonNull String fieldId, @NonNull RelDataType fieldType,
                                       @NonNull RelDataTypeFactory factory) {
        Preconditions.checkArgument(relation.isStruct());
        RelDataTypeBuilder builder = getRelTypeBuilder(factory);
        builder.addAll(relation.getFieldList());
        builder.add(fieldId,fieldType);
        return builder.build();
    }

    @Value
    public static class RelDataTypeFieldBuilder implements RelDataTypeBuilder {

        private final RelDataTypeFactory.FieldInfoBuilder fieldBuilder;

        public RelDataTypeBuilder add(String name, RelDataType type) {
            fieldBuilder.add(name, type);
            return this;
        }

        public RelDataTypeBuilder add(String name, RelDataType type, boolean nullable) {
            fieldBuilder.add(name, type).nullable(nullable);
            return this;
        }

        public RelDataTypeBuilder add(RelDataTypeField field) {
            //TODO: Do we need to do a deep clone or is this kosher since fields are immutable?
            fieldBuilder.add(field);
            return this;
        }

        public RelDataType build() {
            return fieldBuilder.build();
        }

    }



}
