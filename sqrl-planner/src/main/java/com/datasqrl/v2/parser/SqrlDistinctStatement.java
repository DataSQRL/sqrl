package com.datasqrl.v2.parser;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexOver;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.v2.Sqrl2FlinkSQLTranslator;
import com.datasqrl.v2.hint.PlannerHints;

/**
 * SQRL supports a special syntax for converting change streams to state tables which is
 * represented by this class.
 *
 * Most of the complexity is due to a special "filtered distinct" hint that optimizes
 * deduplication for cases where we cannot order on the rowtime. Those have to be planned explicitly.
 */
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


  @Override
public String toSql(Sqrl2FlinkSQLTranslator sqrlEnv, List<StackableStatement> stack) {
    var sql = super.toSql(sqrlEnv, stack);
    var view = sqrlEnv.parseSQL(sql);
    var viewAnalysis = sqrlEnv.analyzeView(view, false, PlannerHints.EMPTY, ErrorCollector.root());
    var relB = viewAnalysis.getRelBuilder();

    //if this is a filtered distinct, we need to add the corresponding processing
    if (isFilteredDistinct && !viewAnalysis.isHasMostRecentDistinct()) {
      /*
      Because we define the view above, we know this is a project->filter->project(rowNum)->logicalwatermark
      The following code extracts those components and the information we need for the filtered distinct
       */
      var project = (LogicalProject)viewAnalysis.getRelNode();
      var filter = (LogicalFilter) project.getInput();
      var rowNum = (LogicalProject) filter.getInput();
      var over = (RexOver) rowNum.getProjects().get(rowNum.getProjects().size()-1); //last one is over
      var window = over.getWindow();
      List<Integer> partition = window.partitionKeys.stream().map(n ->
                CalciteUtil.getInputRef(n).get()).collect(Collectors.toUnmodifiableList());
      var collation = window.orderKeys.get(0);
      int orderIdx = CalciteUtil.getInputRef(collation.getKey()).get();
      relB.push(rowNum.getInput());
      var rowTime = CalciteUtil.findBestRowTimeIndex(relB.peek().getRowType());
      CalciteUtil.addFilteredDeduplication(relB, rowTime.get(), partition, orderIdx);
      relB.project(rowNum.getProjects());
      relB.filter(filter.getCondition());
    } else {
      relB.push(viewAnalysis.getRelNode());
    }
    //Filter out last field for the row number
    relB.project(CalciteUtil.getIdentityRex(relB, relB.peek().getRowType().getFieldCount()-1));
    var rewrittenSQL = sqrlEnv.toSqlString(sqrlEnv.updateViewQuery(sqrlEnv.toSqlNode(relB.build()), view));
    return rewrittenSQL;
  }

}
