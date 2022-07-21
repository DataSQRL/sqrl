package ai.datasqrl.plan.calcite;

import ai.datasqrl.plan.calcite.sqrl.table.CalciteTableFactory;
import ai.datasqrl.schema.builder.AbstractTableFactory;
import ai.datasqrl.schema.input.SqrlTypeConverter;
import ai.datasqrl.schema.input.TableBuilderFlexibleTableConverterVisitor;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.Optional;

@Value
public class CalciteSchemaGenerator extends TableBuilderFlexibleTableConverterVisitor<RelDataType, AbstractTableFactory.UniversalTableBuilder<RelDataType>> {

    SqrlType2Calcite typeConverter;
    RelDataTypeFactory typeFactory;
    CalciteTableFactory tblFactory;

    public CalciteSchemaGenerator(CalciteTableFactory tblFactory) {
        super(tblFactory.getImportFactory());
        this.tblFactory = tblFactory;
        this.typeFactory = tblFactory.getTypeFactory();
        this.typeConverter = new SqrlType2Calcite(typeFactory);
    }

    @Override
    protected Optional<RelDataType> createTable(AbstractTableFactory.UniversalTableBuilder<RelDataType> tblBuilder) {
        return Optional.of(tblFactory.convertTable(tblBuilder,true));
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
