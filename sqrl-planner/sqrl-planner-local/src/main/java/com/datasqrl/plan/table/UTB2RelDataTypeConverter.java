package com.datasqrl.plan.table;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.util.CalciteUtil;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor
public class UTB2RelDataTypeConverter implements UniversalTable.TypeConverter<RelDataType>,
    UniversalTable.SchemaConverter<RelDataType> {

  private final TypeFactory typeFactory;

  @Override
  public RelDataType convertBasic(RelDataType type) {
    return type;
  }

  @Override
  public RelDataType nullable(RelDataType type, boolean nullable) {
    return typeFactory.createTypeWithNullability(type, nullable);
  }

  @Override
  public RelDataType wrapArray(RelDataType type) {

    return typeFactory.createArrayType(type, -1);
  }

  @Override
  public RelDataType nestedTable(List<Pair<String, RelDataType>> fields) {
    CalciteUtil.RelDataTypeBuilder typeBuilder = CalciteUtil.getRelTypeBuilder(
        typeFactory);
    Set<String> names = new HashSet<>();
    for (Pair<String, RelDataType> column : fields) {
      String name = SqlValidatorUtil.uniquify(
          column.getKey(),
          names,
          SqlValidatorUtil.EXPR_SUGGESTER);
      names.add(name);
      typeBuilder.add(name, column.getRight());
    }

    return typeBuilder.build();
  }

  @Override
  public RelDataType convertSchema(UniversalTable tblBuilder) {
    return convertSchema(tblBuilder, true, true);
  }

  public RelDataType convertSchema(UniversalTable tblBuilder, boolean includeNested,
      boolean onlyVisible) {
    return nestedTable(tblBuilder.convert(this, includeNested, onlyVisible));
  }
}
