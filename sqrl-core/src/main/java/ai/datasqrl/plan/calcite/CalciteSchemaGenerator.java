package ai.datasqrl.plan.calcite;

import ai.datasqrl.schema.input.AbstractFlexibleTableConverterVisitor;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.StructKind;

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
        return typeFactory.createArrayType(type,-1L);
    }


}
