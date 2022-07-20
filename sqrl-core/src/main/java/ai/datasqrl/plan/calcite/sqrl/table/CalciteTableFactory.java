package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.CalciteSchemaGenerator;
import ai.datasqrl.schema.builder.AbstractTableFactory;
import ai.datasqrl.schema.builder.VirtualTableFactory;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CalciteTableFactory extends VirtualTableFactory<RelDataType,VirtualSqrlTable> {

    private final AtomicInteger tableIdCounter = new AtomicInteger(0);

    private Name getTableId(Name name) {
        return name.suffix(Integer.toString(tableIdCounter.incrementAndGet()));
    }

    public List<VirtualSqrlTable> createVirtualTables(UniversalTableBuilder<RelDataType> rootTable,
                                                      QuerySqrlTable baseTable,
                                                      CalciteSchemaGenerator schemaGenerator) {
        return build(rootTable, new VirtualTableConstructor(baseTable, schemaGenerator));
    }

    @Override
    protected boolean isTimestamp(RelDataType datatype) {
        return !datatype.isStruct() && datatype.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    @Value
    private final class VirtualTableConstructor implements VirtualTableBuilder<RelDataType,VirtualSqrlTable> {

        QuerySqrlTable baseTable;
        CalciteSchemaGenerator schemaGenerator;

        @Override
        public VirtualSqrlTable make(@NonNull AbstractTableFactory.UniversalTableBuilder<RelDataType> tblBuilder) {
            RelDataType rowType = schemaGenerator.convertTable(tblBuilder,false);
            return new VirtualSqrlTable.Root(getTableId(tblBuilder.getName()), rowType, baseTable);
        }

        @Override
        public VirtualSqrlTable make(@NonNull AbstractTableFactory.UniversalTableBuilder<RelDataType> tblBuilder, VirtualSqrlTable parent, Name shredFieldName) {
            RelDataType rowType = schemaGenerator.convertTable(tblBuilder,false);
            return VirtualSqrlTable.Child.of(getTableId(tblBuilder.getName()),rowType,parent,shredFieldName.getCanonical());
        }
    }


}
