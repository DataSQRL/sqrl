package com.datasqrl.calcite.schema;

import com.datasqrl.calcite.SqrlPreparingTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlNameMatcher;

/**
 * Transforms @.field and @variable to the index of the argument
 */
@Getter
public class TransformArguments extends SqlShuttle {

  private final Optional<SqrlPreparingTable> parentTable;
  private final Optional<SqrlTableFunctionDef> tableDef;
  private final boolean materialSelfTable;
  private final SqlNameMatcher sqlNameMatcher;
  AtomicInteger argCount = new AtomicInteger(0);

  List<SqrlTableParamDef> args = new ArrayList<>();

  public TransformArguments(Optional<SqrlPreparingTable> parentTable, Optional<SqrlTableFunctionDef> tableDef,
      boolean materialSelfTable, SqlNameMatcher sqlNameMatcher) {
    this.parentTable = parentTable;
    this.tableDef = tableDef;
    this.materialSelfTable = materialSelfTable;
    this.sqlNameMatcher = sqlNameMatcher;
    tableDef.ifPresent((d)->args.addAll(d.getParameters()));
  }

  public SqrlTableFunctionDef getArgumentDef() {
    return new SqrlTableFunctionDef(SqlParserPos.ZERO, args);
  }

  /**
   * If we're materializing the self table, replace all of the @.field's with a dynamic param of table arg (create one)
   * If we're not, then only replace variables
   */
  @Override
  public SqlNode visit(SqlIdentifier id) {
    SqlIdentifier replaced = id;
    if (!materialSelfTable && id.names.size() == 2 && id.names.get(0).equalsIgnoreCase("@")) {
      String variableName = String.join("", id.names);
      RelDataType rowType = parentTable.get().getRowType();
      int index = sqlNameMatcher.indexOf(rowType.getFieldNames(), id.names.get(1));
      if (index == -1) {
        throw new RuntimeException("Could not find field: " + id);
      }

      SqlDynamicParam param = new SqlDynamicParam(index, SqlParserPos.ZERO);
      SqlDataTypeSpec spec = new SqlDataTypeSpec(new SqlBasicTypeNameSpec(
          rowType.getFieldList().get(index).getType().getSqlTypeName(), SqlParserPos.ZERO), SqlParserPos.ZERO);

      args.add(new SqrlTableParamDef(SqlParserPos.ZERO,
          new SqlIdentifier(variableName, SqlParserPos.ZERO), spec,
          Optional.of(param), args.size(), true));

      replaced = new SqlIdentifier(variableName, id.getParserPosition());
    }

    if (replaced.names.size() == 1 && replaced.names.get(0).startsWith("@")) {
      final SqlIdentifier ident = replaced;
      SqrlTableParamDef param = this.args.stream()
          .filter(e->e.getName().names.get(0).equalsIgnoreCase(ident.names.get(0)))
          .findFirst().get();

      return new SqlDynamicParam(param.getIndex(), SqlParserPos.ZERO);
    }

    return super.visit(id);
  }

  @Override
  public SqlNode visit(SqlCall call) {
    //don't visit the rhs of the alias
    if (call.getKind() ==SqlKind.AS) {
      return call.getOperator().createCall(call.getParserPosition(),
          call.getOperandList().get(0) instanceof SqlIdentifier
              ?  call.getOperandList().get(0)
              :  call.getOperandList().get(0).accept(this),
          call.getOperandList().get(1));
    }

    return super.visit(call);
  }

  @Override
  public SqlNode visit(SqlNodeList nodeList) {
    //do not process tables
    if (nodeList instanceof SqrlCompoundIdentifier) {
      return nodeList;
    }
    return super.visit(nodeList);
  }
}
