package com.datasqrl.datatype.flink;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.config.TableConfig;
import com.datasqrl.datatype.DataTypeMapper;
import lombok.Getter;
import org.apache.flink.table.functions.UserDefinedFunction;

@Getter
public abstract class FlinkDataTypeMapper implements DataTypeMapper {
  private String engineName;

  public FlinkDataTypeMapper() {
    this.engineName = "flink";
  }

  public abstract boolean isTypeOf(TableConfig tableConfig);

  protected UserDefinedFunction convert(UserDefinedFunction functionDefinition) {
    return functionDefinition;
  }

}
