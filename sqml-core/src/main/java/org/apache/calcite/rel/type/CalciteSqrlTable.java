package org.apache.calcite.rel.type;

import ai.dataeng.sqml.planner.LogicalPlanImpl.DatasetOrTable;
import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

public class CalciteSqrlTable extends AbstractTable {

  private final DatasetOrTable table;
  private final String path;
  private final List<RelDataTypeField> relDataTypeFields;
  DynamicRecordTypeholder holder = null;

  public CalciteSqrlTable(DatasetOrTable table, String path,
      List<RelDataTypeField> relDataTypeFields) {
    this.table = table;
    this.path = path;
    this.relDataTypeFields = relDataTypeFields;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    if (holder == null) {
      this.holder = new DynamicRecordTypeholder(relDataTypeFactory, relDataTypeFields, table);
      return holder;
    }
    return holder;
  }

  public String getPath() {
    return path;
  }

  public DatasetOrTable getTable() {
    return table;
  }

  public DynamicRecordTypeholder getHolder() {
    return holder;
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