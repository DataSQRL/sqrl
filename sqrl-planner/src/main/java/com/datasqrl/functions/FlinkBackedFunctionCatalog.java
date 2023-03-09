package com.datasqrl.functions;

import com.datasqrl.functions.SqrlFunctionCatalog;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.internal.FlinkEnvProxy;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.functions.UserDefinedFunction;

public class FlinkBackedFunctionCatalog implements SqrlFunctionCatalog {

  private final TableEnvironmentImpl tempEnv;

  public FlinkBackedFunctionCatalog() {
    TableEnvironmentImpl tempEnv = TableEnvironmentImpl.create(
        EnvironmentSettings.inStreamingMode().getConfiguration());
    this.tempEnv = tempEnv;
  }

  @Override
  public SqlOperatorTable getOperatorTable() {
    return FlinkEnvProxy.getOperatorTable(tempEnv);
  }

  @Override
  public void addNativeFunction(String name, UserDefinedFunction function) {
    tempEnv.createTemporarySystemFunction(name, function);
  }
}
