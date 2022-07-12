package ai.datasqrl.plan.local;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.calcite.sqrl.table.AbstractSqrlTable;
import ai.datasqrl.plan.local.analyzer.Analysis.ResolvedNamePath;
import ai.datasqrl.plan.local.transpiler.toSql.AliasGenerator;
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
  Map<Name, AbstractSqrlTable> tables;
  JoinPathBuilder joinPathBuilder;


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
    SqrlIdentifier identifier = (SqrlIdentifier)call.getOperandList().get(0);
    ResolvedNamePath namePath = identifier.getNamePath();
    SqlNode from = joinPathBuilder.resolveFull(namePath);

    System.out.println(SqlNodeUtil.printJoin(from));

    List<SqlNode> selectList = new ArrayList<>();
    List<SqlNode> groupBy = new ArrayList<>();
    String subqueryAlias = aliasGenerator.nextTableAliasName().getCanonical();

    //Namepath is relative: sum(_.entries.total)
    if (namePath.getBase().isPresent()) {
      Name originTable = namePath.getBase().get().getToTable().getId();
      List<String> primaryKeys = this.tables.get(originTable).getPrimaryKeys();

      List<SqlNode> pks = primaryKeys.stream().map(pk -> new SqlIdentifier(List.of("_", pk), SqlParserPos.ZERO)).collect(
          Collectors.toList());
      selectList.addAll(pks);

      groupBy.addAll(pks);
    }

    //Add new
    //May be alias
    SqlIdentifier newArg = new SqlIdentifier(List.of(joinPathBuilder.currentAlias,
        namePath.getPath().get(namePath.getPath().size() - 1).getId().getCanonical()), SqlParserPos.ZERO);
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
    return new SqlSelect(SqlParserPos.ZERO, null, selectNodeList, from,
        null, new SqlNodeList(groupBy, SqlParserPos.ZERO), null, null,
        null, null, null,
        new SqlNodeList(SqlParserPos.ZERO));
  }
}
