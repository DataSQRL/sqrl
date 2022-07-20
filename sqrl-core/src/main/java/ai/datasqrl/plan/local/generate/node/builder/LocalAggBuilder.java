package ai.datasqrl.plan.local.generate.node.builder;

import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.calcite.sqrl.table.AbstractSqrlTable;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedNamePath;
import ai.datasqrl.plan.local.generate.QueryGenerator.FieldNames;
import ai.datasqrl.plan.local.generate.node.SqlJoinDeclaration;
import ai.datasqrl.plan.local.generate.node.SqlResolvedIdentifier;
import ai.datasqrl.plan.local.generate.node.util.AliasGenerator;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.VarTable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

@AllArgsConstructor
public class LocalAggBuilder {
  final AliasGenerator aliasGenerator = new AliasGenerator();
  JoinPathBuilder joinPathBuilder;
  FieldNames fieldNames;
  Map<VarTable, AbstractSqrlTable> tableMap;


  /**
   * Pull the function into the to-many subquery
   *
   * sum(o.entries.total)
   * =>
   * SELECT o._uuid, sum(e.total) as genalias
   * FROM Orders o JOIN entries e
   * GROUP BY o._uuid;
   */
  public SqlSelect extractSubquery(SqlCall call) {
    Preconditions.checkState(call.getOperandList().size() == 1);
    Preconditions.checkState(call.getOperandList().get(0) instanceof SqlIdentifier);

    //o.entries.total
    SqlResolvedIdentifier identifier = (SqlResolvedIdentifier)call.getOperandList().get(0);
    ResolvedNamePath namePath = identifier.getNamePath();
    SqlJoinDeclaration from = joinPathBuilder.expandSubquery(namePath);

    List<SqlNode> selectList = new ArrayList<>();
    List<SqlNode> groupBy = new ArrayList<>();
    String subqueryAlias = aliasGenerator.nextTableAliasName().getCanonical();

    //Namepath is relative: sum(_.entries.total)
    if (namePath.getBase().isPresent()) {
      List<String> primaryKeys = tableMap.get(namePath.getBase().get().getToTable()).getPrimaryKeys();

      List<SqlNode> pks = primaryKeys.stream().map(pk -> new SqlIdentifier(List.of("_", pk), SqlParserPos.ZERO)).collect(
          Collectors.toList());
      selectList.addAll(pks);

      groupBy.addAll(pks);
    }

    //Add new
    //May be alias
    SqlIdentifier newArg = new SqlIdentifier(List.of(from.getToTableAlias(),
        fieldNames.get(namePath.getPath().get(namePath.getPath().size() - 1))), SqlParserPos.ZERO);
    SqlCall rewrittenCall = (SqlCall)call.accept(new SqlShuttle(){
      @Override
      public SqlNode visit(SqlIdentifier id) {
        return newArg;
      }
    });
    SqlBasicCall aliasedCall = new SqlBasicCall(SqrlOperatorTable.AS,
        new SqlNode[]{
            rewrittenCall,
            new SqlIdentifier(subqueryAlias, SqlParserPos.ZERO)
        },
        SqlParserPos.ZERO);

    selectList.add(aliasedCall);

    SqlNodeList selectNodeList = new SqlNodeList(selectList, SqlParserPos.ZERO);
    //Get primary key from destination table of alias
    return new SqlSelect(SqlParserPos.ZERO, null, selectNodeList, from.getRel(),
        null, new SqlNodeList(groupBy, SqlParserPos.ZERO), null, null,
        null, null, null,
        new SqlNodeList(SqlParserPos.ZERO));
  }
}
