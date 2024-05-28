package com.datasqrl.graphql.calcite;

import java.lang.reflect.Type;
import java.util.Collection;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SimpleModifiableTable extends AbstractTable implements ScannableTable, ModifiableTable {
    private final List<Object[]> rows = new ArrayList<>();
    private final RelDataType rowType;

    public SimpleModifiableTable(RelDataType rowType) {
        this.rowType = rowType;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return rowType;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return Linq4j.asEnumerable(rows);
    }

    @Override
    public Collection getModifiableCollection() {
        return rows;
    }

    @Override
    public TableModify toModificationRel(RelOptCluster cluster, RelOptTable relOptTable, CatalogReader catalogReader, RelNode child, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened) {
        return new LogicalTableModify(cluster, cluster.traitSet(), relOptTable, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened);
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schemaPlus, String tableName) {
        return new AbstractTableQueryable<T>(queryProvider, schemaPlus, this, tableName) {
            public Enumerator<T> enumerator() {
                // Convert the rows to the appropriate type, e.g., casting or mapping to a class
                return (Enumerator<T>) Linq4j.enumerator(rows);
            }
        };
    }

    @Override
    public Type getElementType() {
        return Object[].class;  // Assuming each row is an Object array; adjust as necessary
    }

    @Override
    public Expression getExpression(SchemaPlus schemaPlus, String tableName, Class clazz) {
        return Schemas.tableExpression(schemaPlus, getElementType(), tableName, clazz);
    }
}
