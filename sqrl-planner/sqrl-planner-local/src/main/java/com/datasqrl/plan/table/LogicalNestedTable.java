package com.datasqrl.plan.table;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

public class LogicalNestedTable extends ScriptRelationalTable {

    @NonNull @Getter
    final ScriptRelationalTable parent;
    final int numLocalPks;
    @Getter
    final int shredIndex;

    final RelDataTypeFactory typeFactory;

    protected LogicalNestedTable(Name nameId, RelDataType rowType, @NonNull ScriptRelationalTable parent, int numLocalPks,
                                 int shredIndex, RelDataTypeFactory typeFactory) {
        super(nameId, rowType);
        this.parent = parent;
        this.numLocalPks = numLocalPks;
        this.shredIndex = shredIndex;
        this.typeFactory = typeFactory;
    }

    @Override
    public boolean isLocked() {
        return getRoot().isLocked();
    }

    @Override
    public void lock() {
        super.lock();
        getRoot().lock();
    }

    @Override
    public RelDataType getRowType() {
        return CalciteUtil.appendField(rowType, ReservedName.SYSTEM_TIMESTAMP.getCanonical(),
                TypeFactory.makeTimestampType(typeFactory, false), typeFactory);
    }

    public PhysicalRelationalTable getRoot() {
        if (parent instanceof PhysicalRelationalTable) {
            return (PhysicalRelationalTable) parent;
        } else {
            return ((LogicalNestedTable)parent).getRoot();
        }
    }

    @Override
    public TableStatistic getTableStatistic() {
        return getRoot().getTableStatistic().nested();
    }

    @Override
    public boolean isRoot() {
        return false;
    }

    public static LogicalNestedTable of(Name nameId, @NonNull RelDataType rowType,
                                                  @NonNull ScriptRelationalTable parent, @NonNull String shredFieldName,
                                                RelDataTypeFactory typeFactory) {
        RelDataTypeField shredField = parent.getRowType().getField(shredFieldName, true, false);
        Preconditions.checkArgument(shredField != null);
        RelDataType type = shredField.getType();
        Preconditions.checkArgument(CalciteUtil.isNestedTable(type));
        //We currently make the hard-coded assumption that children have at most one local primary
        int numLocalPks = CalciteUtil.isArray(type) ? 1 : 0;
        //unwrap if type is in array
        //type = CalciteUtil.getArrayElementType(type).orElse(type);
        LogicalNestedTable child = new LogicalNestedTable(nameId, rowType, parent, numLocalPks, shredField.getIndex(), typeFactory);
        return child;
    }
}
