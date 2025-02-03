package com.datasqrl.v2;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Builder;
import lombok.Value;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.dml.SqlExecute;
import org.apache.flink.sql.parser.dml.SqlStatementSet;
import org.apache.flink.table.api.CompiledPlan;

@Value
@Builder
public class FlinkPhysicalPlan implements EnginePhysicalPlan {

  List<String> flinkSql;
  Set<String> connectors;
  Set<String> formats;
  @JsonIgnore
  String compiledPlan;

  @Value
  public static class Builder {
    List<String> flinkSql = new ArrayList<>();
    List<SqlNode> nodes = new ArrayList<>();
    Set<String> connectors = new HashSet<>();
    Set<String> formats = new HashSet<>();
    List<RichSqlInsert> statementSet = new ArrayList<>();
    //TODO: add compiled plan

    public void addInsert(RichSqlInsert insert) {
      statementSet.add(insert);
    }

    public void add(SqlNode sqlNode, Sqrl2FlinkSQLTranslator sqrlEnv) {
      add(sqlNode, sqrlEnv.toSqlString(sqlNode));
    }

    public void add(SqlNode node, String nodeSql) {
      add(nodeSql);
      nodes.add(node);
      if (node instanceof SqlCreateTable) {
        for (SqlNode option : ((SqlCreateTable)node).getPropertyList().getList()){
          SqlTableOption sqlTableOption = (SqlTableOption)option;
          if (sqlTableOption.getKeyString().equalsIgnoreCase("connector")) {
            connectors.add(sqlTableOption.getValueString());
          }
          switch (sqlTableOption.getKeyString()) {
            case "format":
            case "key.format":
            case "value.format":
              formats.add(sqlTableOption.getValueString());
          }
        }
      }
    }

    private void add(String sql) {
      flinkSql.add(sql);
    }

    public SqlExecute getExecuteStatement() {
      SqlStatementSet sqlStatementSet = new SqlStatementSet(statementSet, SqlParserPos.ZERO);
      return new SqlExecute(sqlStatementSet, SqlParserPos.ZERO);
    }

    public FlinkPhysicalPlan build(CompiledPlan compiledPlan) {
      return new FlinkPhysicalPlan(flinkSql, connectors, formats, compiledPlan.asJsonString());
    }


  }


}
