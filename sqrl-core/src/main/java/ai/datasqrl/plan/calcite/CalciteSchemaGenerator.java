package ai.datasqrl.plan.calcite;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.input.AbstractFlexibleTableConverterVisitor;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import ai.datasqrl.schema.type.Type;
import ai.datasqrl.schema.type.basic.*;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.types.DataType;


import java.util.List;
import java.util.Optional;

@Value
public class CalciteSchemaGenerator extends AbstractFlexibleTableConverterVisitor<RelDataType> {

    RelDataTypeFactory typeFactory;
    SqrlType2Calcite typeConverter;

    public CalciteSchemaGenerator(RelDataTypeFactory typeFactory) {
        this.typeFactory = typeFactory;
        this.typeConverter = new SqrlType2Calcite(typeFactory);
    }

    @Override
    protected Optional<RelDataType> createTable(TableBuilder<RelDataType> tblBuilder) {
        RelDataTypeFactory.FieldInfoBuilder fieldBuilder = typeFactory.builder().kind(StructKind.FULLY_QUALIFIED);
        for (TableBuilder.Column<RelDataType> column : tblBuilder.getColumns()) {
            fieldBuilder.add(column.getName().getCanonical(), column.getType()).nullable(column.isNotNull());
        };
        return Optional.of(fieldBuilder.build());
    }

    @Override
    public RelDataType nullable(RelDataType type, boolean notnull) {
        return type; //Does not support nullability at the type level, but only field
    }

    @Override
    protected SqrlTypeConverter<RelDataType> getTypeConverter() {
        return typeConverter;
    }

    @Override
    public RelDataType wrapArray(RelDataType type, boolean notnull) {
        return typeFactory.createArrayType(type,Short.MAX_VALUE);
    }


}
