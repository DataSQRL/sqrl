package com.datasqrl.flink.function;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

public class DelegatingSqlCallBinding extends SqlCallBinding {

    private final FlinkTypeFactory flinkTypeFactory;

    public DelegatingSqlCallBinding(FlinkTypeFactory flinkTypeFactory, SqlCallBinding sqlCallBinding) {
      super(sqlCallBinding.getValidator(), sqlCallBinding.getScope(), sqlCallBinding.getCall());
      this.flinkTypeFactory = flinkTypeFactory;
    }

    @Override
    public RelDataTypeFactory getTypeFactory() {
      return flinkTypeFactory;
    }
}