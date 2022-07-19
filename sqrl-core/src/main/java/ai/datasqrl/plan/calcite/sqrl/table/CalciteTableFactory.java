package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.CalciteSchemaGenerator;
import ai.datasqrl.schema.table.TableProxy;
import ai.datasqrl.schema.table.TableProxyFactory;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public class CalciteTableFactory extends TableProxyFactory<RelDataType,VirtualSqrlTable> {

    public List<TableProxy<VirtualSqrlTable>> createProxyTables(TableProxyFactory.TableBuilder<RelDataType> rootTable,
                                                                  QuerySqrlTable baseTable,
                                                      CalciteSchemaGenerator schemaGenerator) {
        return build(rootTable, new VirtualTableConstructor(baseTable, schemaGenerator));
    }

    @Override
    protected boolean isTimestamp(RelDataType datatype) {
        return !datatype.isStruct() && datatype.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    @Value
    private final static class VirtualTableConstructor implements VirtualTableBuilder<RelDataType,VirtualSqrlTable> {

        QuerySqrlTable baseTable;
        CalciteSchemaGenerator schemaGenerator;

        @Override
        public VirtualSqrlTable make(@NonNull TableBuilder<RelDataType> tblBuilder) {
            RelDataType rowType = schemaGenerator.convertTable(tblBuilder,false);
            return new VirtualSqrlTable.Root(tblBuilder.getId(), rowType, baseTable);
        }

        @Override
        public VirtualSqrlTable make(@NonNull TableBuilder<RelDataType> tblBuilder, VirtualSqrlTable parent, Name shredFieldName) {
            RelDataType rowType = schemaGenerator.convertTable(tblBuilder,false);
            return VirtualSqrlTable.Child.of(tblBuilder.getId(),rowType,parent,shredFieldName.getCanonical());
        }
    }

}
