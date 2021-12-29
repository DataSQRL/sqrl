package org.apache.calcite.rel.type;

import ai.dataeng.sqml.planner.DatasetOrTable;
import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.schema.CustomColumnResolvingTable;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

/**
 * A dynamically expanding table. This is a single use table.
 */
public class CalciteTable implements CustomColumnResolvingTable {

  private final DatasetOrTable table;
  /**
   * We can arrive at a table though multiple paths. We need to keep track
   * of the fields discovered on each unique logical table as well as the
   * path so we can rewrite it later.
   */
  private final String originalTableName;
  private final List<RelDataTypeField> relDataTypeFields;
  GrowableRecordType holder = null;

  public CalciteTable(DatasetOrTable table, String originalTableName,
      List<RelDataTypeField> relDataTypeFields) {
    this.table = table;
    this.originalTableName = originalTableName;
    this.relDataTypeFields = relDataTypeFields;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    if (holder == null) {
      this.holder = new GrowableRecordType(relDataTypeFactory, relDataTypeFields, table);
      return holder;
    }
    return holder;
  }

  public String getOriginalTableName() {
    return originalTableName;
  }

  public DatasetOrTable getTable() {
    return table;
  }

  public GrowableRecordType getHolder() {
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

  @Override
  public List<Pair<RelDataTypeField, List<String>>> resolveColumn(RelDataType relDataType,
      RelDataTypeFactory relDataTypeFactory, List<String> list) {

    if (list.size() > 1) {
      String path = String.join(".", list);
      RelDataTypeField resolvedField = holder.getField(path, false, false);
      return List.of(
          Pair.of(resolvedField, List.of())
      );
    }

    return List.of(
        Pair.of(holder.getField(list.get(0), false, false), List.of())
    );
  }
}