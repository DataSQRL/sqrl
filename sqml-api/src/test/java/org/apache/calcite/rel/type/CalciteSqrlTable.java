package org.apache.calcite.rel.type;

import ai.dataeng.sqml.logical4.LogicalPlan.DatasetOrTable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

public class CalciteSqrlTable extends AbstractTable {

  private final DatasetOrTable table;

  public CalciteSqrlTable(DatasetOrTable table) {
    this.table = table;
    System.out.println("CALCITE TABLE " + table);
  }
//    List<RelDataTypeField> regFields = new ArrayList<>();
    DynamicRecordTypeholder holder = null;

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
      if (holder == null) {
        this.holder = new DynamicRecordTypeholder(relDataTypeFactory, new ArrayList<>(), table);
        return holder;
      }
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
//
//    Map<List<String>, RelDataTypeField> map = new HashMap<>();
////    @Override
//    public List<Pair<RelDataTypeField, List<String>>> resolveColumn(RelDataType relDataType,
//        RelDataTypeFactory relDataTypeFactory, List<String> list) {
//      if (map.containsKey(list)) {
//        return List.of(Pair.of(map.get(list), list.subList(1, list.size())));
//      }
//      RelDataTypeField f;
//
//      if (list.get(0).equalsIgnoreCase("parent.time")) {
//        f = new RelDataTypeFieldImpl(list.get(0), 0,
//            relDataTypeFactory.createSqlType(SqlTypeName.INTERVAL_DAY));
//      } else if(list.get(0).equalsIgnoreCase("quantity")) {
//        f = new RelDataTypeFieldImpl(list.get(0), 0,
//            relDataTypeFactory.createSqlType(SqlTypeName.INTEGER));
//      } else {
//
//        //todo: We must walk the path
//        f = new RelDataTypeFieldImpl(list.get(0), 0,
//            relDataTypeFactory.createSqlType(SqlTypeName.BOOLEAN));
//      }
//      map.put(list, f);
////      regFields.add(f);
//      return List.of(Pair.of(f, list.subList(1, list.size())));
//    }
  }