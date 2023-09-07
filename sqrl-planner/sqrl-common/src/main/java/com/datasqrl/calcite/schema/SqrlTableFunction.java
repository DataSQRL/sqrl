package com.datasqrl.calcite.schema;

import com.datasqrl.calcite.CatalogReader;
import com.datasqrl.calcite.SqrlRelBuilder;
import java.lang.reflect.Type;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlNode;

@Getter
public class SqrlTableFunction implements TableFunction {
  List<FunctionParameter> parameters;
  SqlNode node;
  private final String tableName;
  private final CatalogReader catalogReader;

  public SqrlTableFunction(List<FunctionParameter> parameters, SqlNode node,
      String tableName, CatalogReader catalogReader) {
    this.parameters = parameters;
    this.node = node;
    this.tableName = tableName;
    this.catalogReader = catalogReader;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<Object> list) {
    return SqrlRelBuilder.shadow(catalogReader.getTable(List.of(tableName)).getRowType());
  }

  @Override
  public Type getElementType(List<Object> list) {
    return Object.class;
  }
}
