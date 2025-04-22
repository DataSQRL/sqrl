package com.datasqrl.datatype.flink;

import org.apache.flink.table.functions.UserDefinedFunction;

import com.datasqrl.config.TableConfig;
import com.datasqrl.datatype.DataTypeMapper;

import lombok.Getter;

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
