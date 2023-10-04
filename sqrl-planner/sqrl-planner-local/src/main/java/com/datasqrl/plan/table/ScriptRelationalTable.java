package com.datasqrl.plan.table;

import com.datasqrl.calcite.ModifiableTable;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.schema.SQRLTable;
import com.google.common.collect.ContiguousSet;
import java.lang.reflect.Type;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public abstract class ScriptRelationalTable extends AbstractRelationalTable
    implements ModifiableTable, QueryableTable {

    /**
     * The data type for the (possibly shredded) table represented by this virtual table
     */
    @NonNull
    protected RelDataType rowType;

    @NonNull
    private final List<AddedColumn> addedColumns = new ArrayList<>();

    /**
     * When a table is locked, no more columns can be added
     */
    @Getter
    private boolean isLocked;

    //TODO: This needs to be removed
    @Getter @Setter
    private SQRLTable sqrlTable;

    protected ScriptRelationalTable(@NonNull Name nameId, @NonNull RelDataType rowType) {
        super(nameId);
        this.rowType = rowType;
    }

    public void lock() {
        this.isLocked = true;
    }

    @Override
    public RelDataType getRowType() {
        return rowType;
    }

    public int getNumColumns() {
        return getRowType().getFieldCount();
    }

    public int addColumn(@NonNull AddedColumn column, @NonNull RelDataTypeFactory typeFactory) {
        if (isLocked()) throw new TableLockedException(this);
        int index = getNumColumns();
        addedColumns.add(column);
        rowType = column.appendTo(rowType, typeFactory);
        return index;
    }

    public Iterable<AddedColumn> getAddedColumns() {
        return addedColumns;
    }

    @Override
    public int addColumn(String name, RexNode column, RelDataTypeFactory typeFactory) {
        return addColumn(new AddedColumn(name, column), typeFactory);
    }

    public abstract TableStatistic getTableStatistic();

    public abstract int getNumPrimaryKeys();

    public int getNumLocalPks() {
        return getNumPrimaryKeys();
    }

    public abstract boolean isRoot();

    public abstract PhysicalRelationalTable getRoot();

    public Statistic getStatistic() {
        TableStatistic tableStatistic = getTableStatistic();
        if (tableStatistic.isUnknown()) {
            return Statistics.UNKNOWN;
        }
        ImmutableBitSet primaryKey = ImmutableBitSet.of(ContiguousSet.closedOpen(0, getNumPrimaryKeys()));
        return Statistics.of(tableStatistic.getRowCount(), List.of(primaryKey));
    }

    @AllArgsConstructor
    public static class TableLockedException extends RuntimeException {

        private ScriptRelationalTable table;

    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schemaPlus,
        String s) {
        return null;
    }

    @Override
    public Type getElementType() {
        return null;
    }
}
