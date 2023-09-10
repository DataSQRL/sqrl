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
public class SqrlTableFunction implements TableFunction, CustomColumnResolvingTable {
  List<FunctionParameter> parameters;
  SqlNode node;
  private final String tableName;
  private final CatalogReader catalogReader;
  private final Optional<RelDataType> typeOptional;

  public SqrlTableFunction(List<FunctionParameter> parameters, SqlNode node,
      String tableName, CatalogReader catalogReader, Optional<RelDataType> typeOptional) {
    this.parameters = parameters;
    this.node = node;
    this.tableName = tableName;
    this.catalogReader = catalogReader;
    this.typeOptional = typeOptional;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<Object> list) {
    PreparingTable table = catalogReader.getTable(List.of(tableName));
    ;
    if (table != null) {
      return table.getRowType();
    } else {
      return typeOptional.get();
    }
  }

  @Override
  public Type getElementType(List<Object> list) {
    return Object.class;
  }

  @Override
  public List<Pair<RelDataTypeField, List<String>>> resolveColumn(RelDataType relDataType,
      RelDataTypeFactory relDataTypeFactory, List<String> list) {
    return null;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return getRowType(relDataTypeFactory, List.of());
  }

  @Override
  public Statistic getStatistic() {
    return null;
  }

  @Override
  public TableType getJdbcTableType() {
    return null;
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
}
