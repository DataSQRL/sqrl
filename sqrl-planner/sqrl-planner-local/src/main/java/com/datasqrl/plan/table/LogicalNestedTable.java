package com.datasqrl.plan.table;

import com.datasqrl.canonicalizer.Name;
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

    protected LogicalNestedTable(Name nameId, RelDataType rowType, @NonNull ScriptRelationalTable parent, int numLocalPks, int shredIndex) {
        super(nameId, rowType);
        this.parent = parent;
        this.numLocalPks = numLocalPks;
        this.shredIndex = shredIndex;
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

    public int getNumParentPks() {
        return parent.getNumPrimaryKeys();
    }

    @Override
    public int getNumPrimaryKeys() {
        return parent.getNumPrimaryKeys() + numLocalPks;
    }

    @Override
    public int getNumLocalPks() {
        return numLocalPks;
    }

    @Override
    public boolean isRoot() {
        return false;
    }

    public void appendTimestampColumn(@NonNull RelDataTypeFactory typeFactory) {
        PhysicalRelationalTable base = getRoot();
        int timestampIdx = base.getTimestamp().getTimestampCandidate().getIndex();
        RelDataTypeField timestampField = base.getRowType().getFieldList().get(timestampIdx);
        rowType = CalciteUtil.appendField(rowType, timestampField.getName(),
                timestampField.getType(), typeFactory);
    }

    public static LogicalNestedTable of(Name nameId, @NonNull RelDataType rowType,
                                                  @NonNull ScriptRelationalTable parent, @NonNull String shredFieldName) {
        RelDataTypeField shredField = parent.getRowType().getField(shredFieldName, true, false);
        Preconditions.checkArgument(shredField != null);
        RelDataType type = shredField.getType();
        Preconditions.checkArgument(CalciteUtil.isNestedTable(type));
        //We currently make the hard-coded assumption that children have at most one local primary
        int numLocalPks = CalciteUtil.getArrayElementType(type).isPresent() ? 1 : 0;
        //unwrap if type is in array
        //type = CalciteUtil.getArrayElementType(type).orElse(type);
        LogicalNestedTable child = new LogicalNestedTable(nameId, rowType, parent, numLocalPks, shredField.getIndex());
        return child;
    }
}
