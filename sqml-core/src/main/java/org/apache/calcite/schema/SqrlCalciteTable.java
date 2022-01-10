package org.apache.calcite.schema;

import ai.dataeng.sqml.planner.DatasetOrTable;
import ai.dataeng.sqml.planner.optimize2.SqrlLogicalTableScan;
import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.GrowableRecordType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

/**
 * A dynamically expanding table.
 */
public class SqrlCalciteTable implements CustomColumnResolvingTable
    , TranslatableTable
{

  private final DatasetOrTable table;
  /**
   * We can arrive at a table though multiple paths. We need to keep track
   * of the fields discovered on each unique logical table as well as the
   * path so we can rewrite it later.
   */
  private final String originalTableName;
  private final List<RelDataTypeField> relDataTypeFields;
  GrowableRecordType holder = null;

  public SqrlCalciteTable(DatasetOrTable table, String originalTableName,
      List<RelDataTypeField> relDataTypeFields) {
    this.table = table;
    this.originalTableName = originalTableName;
    this.relDataTypeFields = relDataTypeFields;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    if (holder == null) {
      this.holder = new GrowableRecordType(relDataTypeFields, table);
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

    //Consume the entire field
    String path = String.join(".", list);
    RelDataTypeField resolvedField = holder.getField(path, false, false);
    return List.of(
        Pair.of(resolvedField, List.of())
    );
  }

  @Override
  public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
    RelOptCluster cluster = context.getCluster();

    SqrlLogicalTableScan scan = new SqrlLogicalTableScan(cluster, cluster.traitSet(), List.of(),
        relOptTable, ((ai.dataeng.sqml.planner.Table)table).getPrimaryKeys());

    return scan;
  }
}