package org.apache.calcite.rel.type;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.TableResolver;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

@AllArgsConstructor
public class OrdersSchema implements Schema {
    TableResolver tableResolver;

  public static class SqrlTable extends AbstractTable {

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
      return
          relDataTypeFactory.createStructType(StructKind.PEEK_FIELDS, List.of(
          new DynamicRecordTypeImpl2(relDataTypeFactory),
          relDataTypeFactory.createUnknownType()
        ),
          List.of("orders", "scalar"));
    }

    @Override
    public Statistic getStatistic() {
      return Statistics.UNKNOWN;
    }

    @Override
    public TableType getJdbcTableType() {
      return TableType.TABLE;
    }

    @Override
    public boolean isRolledUp(String s) {
      return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String s, SqlCall sqlCall, SqlNode sqlNode,
        CalciteConnectionConfig calciteConnectionConfig) {
      return false;
    }
  }

  @Override
  public Table getTable(String s) {
    System.out.println("Table: " +s);
    return tableResolver.resolve(s);
  }

  @Override
  public Set<String> getTableNames() {
    return Set.of("entries", "a");
  }

  @Override
  public RelProtoDataType getType(String s) {
    return null;
  }

  @Override
  public Set<String> getTypeNames() {
    return Set.of();
  }

  @Override
  public Collection<Function> getFunctions(String s) {
    return null;
  }

  @Override
  public Set<String> getFunctionNames() {
    return Set.of();
  }

  @Override
  public Schema getSubSchema(String s) {
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Set.of();
  }

  @Override
  public Expression getExpression(SchemaPlus schemaPlus, String s) {
    return null;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public Schema snapshot(SchemaVersion schemaVersion) {
    return null;
  }
}
