/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.planner.parser;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.datasqrl.planner.hint.HintsAndDoc;
import com.datasqrl.util.CalciteUtil;
import java.util.List;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexOver;

/**
 * SQRL supports a special syntax for converting change streams to state tables which is represented
 * by this class.
 *
 * <p>Most of the complexity is due to a special "filtered distinct" hint that optimizes
 * deduplication for cases where we cannot order on the rowtime. Those have to be planned
 * explicitly.
 */
public class SqrlDistinctStatement extends SqrlDefinition {

  // The DISTINCT statement is rewritten into this template around the ON columns, ORDER BY clause
  // and FROM table of the original statement
  private static final String COLUMNS_PREFIX =
      "SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY ";
  private static final String ORDER_PREFIX = "  ORDER BY ";
  private static final String FROM_PREFIX = ") AS __sqrlinternal_rownum FROM ";
  private static final String SUFFIX = ") WHERE __sqrlinternal_rownum=1";

  final boolean isFilteredDistinct;
  private final ParsedObject<NamePath> from;
  private final ParsedObject<String> columns;
  private final ParsedObject<String> remaining;

  public SqrlDistinctStatement(
      ParsedObject<NamePath> tableName,
      SqrlComments comments,
      AccessModifier access,
      ParsedObject<NamePath> from,
      ParsedObject<String> columns,
      ParsedObject<String> remaining) {
    super(
        tableName,
        new ParsedObject<>(
            COLUMNS_PREFIX
                + columns.get()
                + ORDER_PREFIX
                + remaining.get()
                + FROM_PREFIX
                + from.get()
                + SUFFIX,
            columns.getFileLocation()),
        access,
        comments.removeHintsByName(FILTERED_DISTINCT_HINT_NAME::equalsIgnoreCase));
    isFilteredDistinct = comments.containsHintByName(FILTERED_DISTINCT_HINT_NAME::equalsIgnoreCase);
    this.from = from;
    this.columns = columns;
    this.remaining = remaining;
  }

  /**
   * Maps locations in the rewritten SQL back to the ON columns or the ORDER BY clause of the
   * original statement, and all other locations to the FROM table.
   */
  @Override
  public FileLocation mapSqlLocation(FileLocation location) {
    var index = toIndex(getPrefix() + definitionBody.get(), location) - getPrefix().length();
    var columnsStart = COLUMNS_PREFIX.length();
    var orderStart = columnsStart + columns.get().length() + ORDER_PREFIX.length();
    if (index >= columnsStart && index <= columnsStart + columns.get().length()) {
      return SqrlStatementParser.relativeLocation(columns, index - columnsStart);
    }
    if (index >= orderStart && index <= orderStart + remaining.get().length()) {
      return SqrlStatementParser.relativeLocation(remaining, index - orderStart);
    }
    return from.getFileLocation();
  }

  private static int toIndex(String text, FileLocation location) {
    var index = 0;
    var line = 1;
    while (line < location.line() && index < text.length()) {
      if (text.charAt(index++) == '\n') {
        line++;
      }
    }
    return index + location.offset() - 1;
  }

  public static final String FILTERED_DISTINCT_HINT_NAME = "filtered_distinct_order";

  @Override
  public String toSql(Sqrl2FlinkSQLTranslator sqrlEnv, List<StackableStatement> stack) {
    var sql = super.toSql(sqrlEnv, stack);
    var view = sqrlEnv.parseSQL(sql);
    var viewAnalysis = sqrlEnv.analyzeView(view, false, HintsAndDoc.EMPTY, ErrorCollector.root());
    var relB = viewAnalysis.relBuilder();

    // if this is a filtered distinct, we need to add the corresponding processing
    if (isFilteredDistinct && !viewAnalysis.hasMostRecentDistinct()) {
      /*
      Because we define the view above, we know this is a project->filter->project(rowNum)->logicalwatermark
      The following code extracts those components and the information we need for the filtered distinct
       */
      var project = (LogicalProject) viewAnalysis.relNode();
      var filter = (LogicalFilter) project.getInput();
      var rowNum = (LogicalProject) filter.getInput();
      var over =
          (RexOver) rowNum.getProjects().get(rowNum.getProjects().size() - 1); // last one is over
      var window = over.getWindow();
      List<Integer> partition =
          window.partitionKeys.stream().map(n -> CalciteUtil.getInputRef(n).get()).toList();
      var collation = window.orderKeys.get(0);
      int orderIdx = CalciteUtil.getInputRef(collation.getKey()).get();
      relB.push(rowNum.getInput());
      var rowTime = CalciteUtil.findBestRowTimeIndex(relB.peek().getRowType());
      CalciteUtil.addFilteredDeduplication(relB, rowTime.get(), partition, orderIdx);
      relB.project(rowNum.getProjects());
      relB.filter(filter.getCondition());
    } else {
      relB.push(viewAnalysis.relNode());
    }
    // Filter out last field for the row number
    relB.project(CalciteUtil.getIdentityRex(relB, relB.peek().getRowType().getFieldCount() - 1));
    var rewrittenSql = sqrlEnv.updateViewQuery(RelToFlinkSql.convertToSqlNode(relB.build()), view);

    return RelToFlinkSql.convertToString(rewrittenSql);
  }
}
