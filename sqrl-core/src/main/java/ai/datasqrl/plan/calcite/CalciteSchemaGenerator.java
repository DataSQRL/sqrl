package ai.datasqrl.plan.calcite;

import ai.datasqrl.plan.calcite.sqrl.table.CalciteTableFactory;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.schema.input.TableBuilderFlexibleTableConverterVisitor;
import ai.datasqrl.schema.builder.AbstractTableFactory;
import ai.datasqrl.schema.builder.NestedTableBuilder;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.List;
import java.util.Optional;

@Value
public class CalciteSchemaGenerator extends TableBuilderFlexibleTableConverterVisitor<RelDataType, AbstractTableFactory.UniversalTableBuilder<RelDataType>> {

    RelDataTypeFactory typeFactory;
    SqrlType2Calcite typeConverter;
    CalciteTableFactory tblFactory;

    public CalciteSchemaGenerator(RelDataTypeFactory typeFactory, CalciteTableFactory tblFactory) {
        super(tblFactory.getImportFactory());
        this.tblFactory = tblFactory;
        this.typeFactory = typeFactory;
        this.typeConverter = new SqrlType2Calcite(typeFactory);
    }

    @Override
    protected Optional<RelDataType> createTable(AbstractTableFactory.UniversalTableBuilder<RelDataType> tblBuilder) {
        return Optional.of(convertTable(tblBuilder,true));
    }

    public RelDataType convertTable(AbstractTableFactory.UniversalTableBuilder<RelDataType> tblBuilder, boolean forNested) {
        CalciteUtil.RelDataTypeBuilder typeBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
        List<NestedTableBuilder.Column<RelDataType>> columns = tblBuilder.getColumns(forNested,forNested);
        for (NestedTableBuilder.Column<RelDataType> column : columns) {
            typeBuilder.add(column.getId(), column.getType(), column.isNullable());
        };
        return typeBuilder.build();
    }

    @Override
    public RelDataType nullable(RelDataType type, boolean nullable) {
        return typeFactory.createTypeWithNullability(type, nullable);
    }

    @Override
    protected SqrlTypeConverter<RelDataType> getTypeConverter() {
        return typeConverter;
    }

    @Override
    public RelDataType wrapArray(RelDataType type, boolean nullable) {
        return typeFactory.createArrayType(nullable(type,nullable),-1L);
    }


}
