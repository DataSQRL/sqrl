package org.apache.calcite.schema;

import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

/**
 * No unsqrl calcite schema
 */
@AllArgsConstructor
public class SqrlSchema2 extends AbstractSqrlSchema {

  @Override
  public Table getTable(String table) {

    return new SqrlTable2();
  }

  public class SqrlTable2
      extends RelDataTypeImpl
      implements CustomColumnResolvingTable, TranslatableTable {

    @Override
    protected void generateTypeString(StringBuilder stringBuilder, boolean b) {

    }

    @Override
    public List<Pair<RelDataTypeField, List<String>>> resolveColumn(RelDataType relDataType,
        RelDataTypeFactory relDataTypeFactory, List<String> list) {
      return null;
    }

    @Override
    public RelNode toRel(ToRelContext toRelContext, RelOptTable relOptTable) {
      return null;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
      return null;
    }

    @Override
    public Statistic getStatistic() {
      return null;
    }

    @Override
    public TableType getJdbcTableType() {
      return null;
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
}
