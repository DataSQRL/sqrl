package com.datasqrl.plan.table;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

import com.datasqrl.calcite.ModifiableTable;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.util.CalciteUtil;

import lombok.Getter;
import lombok.NonNull;

public abstract class ScriptRelationalTable extends AbstractRelationalTable
    implements ModifiableTable, QueryableTable {

    /**
     * The data type for the (possibly shredded) table represented by this virtual table
     */
    @NonNull
    protected RelDataType rowType;

    /**
     * The number of selected columns. The first columns are the ones the user actively selected.
     * Additional metadata columns may have been added at the end.
     */
    @Getter
    protected int numSelects;

    @NonNull
    private final List<AddedColumn> addedColumns = new ArrayList<>();

    /**
     * When a table is locked, no more columns can be added
     */
    @Getter
    private boolean isLocked;

    protected ScriptRelationalTable(@NonNull Name nameId, @NonNull RelDataType rowType, int numSelects) {
        super(nameId);
        this.rowType = rowType;
        this.numSelects = numSelects;
    }

    public void lock() {
        this.isLocked = true;
    }

    @Override
    public RelDataType getRowType() {
        return rowType;
    }

    @Override
    public int getNumColumns() {
        return getRowType().getFieldCount();
    }



    public int addColumn(@NonNull AddedColumn column, @NonNull RelDataTypeFactory typeFactory) {
        if (isLocked()) {
			throw new UnsupportedOperationException("Table is locked: " + getNameId());
		}
        var index = numSelects;
        numSelects++;
        addedColumns.add(column);
        rowType = CalciteUtil.addField(rowType, index, column.getNameId(), column.getDataType(), typeFactory);
        return index;
    }

    public List<AddedColumn> getAddedColumns() {
        return addedColumns;
    }

    @Override
    public int addColumn(String name, RexNode column, RelDataTypeFactory typeFactory) {
        return addColumn(new AddedColumn(name, column), typeFactory);
    }

    public abstract TableStatistic getTableStatistic();

    public abstract boolean isRoot();

    public abstract PhysicalRelationalTable getRoot();

    // An empty queryable table, used for tests
    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
        String tableName) {
        return new AbstractTableQueryable<>(queryProvider, schema,
            this,
            tableName) {
            @Override
            public Enumerator<T> enumerator() {
                return queryProvider.executeQuery(this);
            }
        };
    }

    @Override
    public Type getElementType() {
        return Object[].class;
    }
}
