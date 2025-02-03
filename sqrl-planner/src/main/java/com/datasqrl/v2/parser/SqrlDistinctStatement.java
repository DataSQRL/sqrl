package com.datasqrl.v2.parser;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.v2.Sqrl2FlinkSQLTranslator;
import com.datasqrl.v2.analyzer.SQRLLogicalPlanAnalyzer.ViewAnalysis;
import com.datasqrl.v2.hint.PlannerHints;
import com.datasqrl.util.CalciteUtil;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelBuilder;

public class SqrlDistinctStatement extends SqrlDefinition {

  final boolean isFilteredDistinct;

  public SqrlDistinctStatement(
      ParsedObject<NamePath> tableName,
      SqrlComments comments,
      AccessModifier access,
      ParsedObject<NamePath> from,
      ParsedObject<String> columns,
      ParsedObject<String> remaining) {
    super(tableName,
        new ParsedObject<>(String.format("SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY %s "
                + " ORDER BY %s) AS __sqrlinternal_rownum FROM %s) WHERE __sqrlinternal_rownum=1",
                                          columns.get(), remaining.get(), from.get()),
            columns.getFileLocation()), access,
        comments.removeHintsByName(FILTERED_DISTINCT_HINT_NAME::equalsIgnoreCase));
    isFilteredDistinct = comments.containsHintByName(FILTERED_DISTINCT_HINT_NAME::equalsIgnoreCase);
  }

  public static final String FILTERED_DISTINCT_HINT_NAME = "filtered_distinct_order";


  public String toSql(Sqrl2FlinkSQLTranslator sqrlEnv, List<StackableStatement> stack) {
    String sql = super.toSql(sqrlEnv, stack);
    System.out.println(sql);
    SqlNode view = sqrlEnv.parseSQL(sql);
    ViewAnalysis viewAnalysis = sqrlEnv.analyzeView(view, false, PlannerHints.EMPTY, ErrorCollector.root());
    RelBuilder relB = viewAnalysis.getRelBuilder();

    //Rewrite statement
    if (isFilteredDistinct) {
      //Because we define the view above, we know this is a project->filter->project(rowNum)->logicalwatermark
      LogicalProject project = (LogicalProject)viewAnalysis.getRelNode();
      LogicalFilter filter = (LogicalFilter) project.getInput();
      LogicalProject rowNum = (LogicalProject) filter.getInput();
      RexOver over = (RexOver) rowNum.getProjects().get(rowNum.getProjects().size()-1); //last one is over
      RexWindow window = over.getWindow();
      List<Integer> partition = window.partitionKeys.stream().map(n ->
                CalciteUtil.getInputRef(n).get()).collect(Collectors.toUnmodifiableList());
      RexFieldCollation collation = window.orderKeys.get(0);
      int orderIdx = CalciteUtil.getInputRef(collation.getKey()).get();
      relB.push(rowNum.getInput());
      Optional<Integer> rowTime = CalciteUtil.findBestRowTimeIndex(relB.peek().getRowType());
      CalciteUtil.addFilteredDeduplication(relB, rowTime.get(), partition, orderIdx);
      relB.project(rowNum.getProjects());
      relB.filter(filter.getCondition());
    } else {
      relB.push(viewAnalysis.getRelNode());
    }
    //Filter out last field
    relB.project(CalciteUtil.getIdentityRex(relB, relB.peek().getRowType().getFieldCount()-1));
    String rewrittenSQL = sqrlEnv.toSqlString(sqrlEnv.updateViewQuery(sqrlEnv.toSqlNode(relB.build()), view));
    System.out.println("REWRITTEN: " + rewrittenSQL);
    return rewrittenSQL;
  }

}
