package com.datasqrl.datatype.flink;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.config.TableConfig;
import com.datasqrl.datatype.DataTypeMapper;
import com.datasqrl.flink.FlinkConverter;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.SqlFunction;
import org.apache.flink.table.functions.FunctionDefinition;

@Getter
public abstract class FlinkDataTypeMapper implements DataTypeMapper {
  private String engineName;

  public FlinkDataTypeMapper() {
    this.engineName = "flink";
  }

  public abstract boolean isTypeOf(TableConfig tableConfig);

  protected SqlFunction convert(FunctionDefinition functionDefinition) {
    FlinkConverter flinkConverter = new FlinkConverter(TypeFactory.getTypeFactory());

    Optional<SqlFunction> convertedFunction = flinkConverter.convertFunction(
        functionDefinition.getClass().getSimpleName(),
        functionDefinition);
    return convertedFunction.get();
  }

}
