package com.datasqrl.calcite.schema;

import com.datasqrl.calcite.CatalogReader;
import com.datasqrl.calcite.SqrlRelBuilder;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.prepare.Prepare.PreparingTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.CustomColumnResolvingTable;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;

@Getter
@AllArgsConstructor
public class SqrlTableFunction implements TableFunction {
  private final List<FunctionParameter> parameters;
  private final SqlNode node;
  private final String tableName;
  private final CatalogReader catalogReader;
  private final Optional<RelDataType> typeOptional;

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<Object> list) {
    //Look up most recent table
    PreparingTable table = catalogReader.getTable(List.of(tableName));
    if (table != null) {
      return table.getRowType();
    } else {
      //here for query access tables (not registered as a table)
      return typeOptional.get();
    }
  }

  @Override
  public Type getElementType(List<Object> list) {
    return Object[].class;
  }
}
