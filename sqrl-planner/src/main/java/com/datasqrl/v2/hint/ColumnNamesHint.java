package com.datasqrl.v2.hint;

import com.datasqrl.v2.parser.ParsedObject;
import com.datasqrl.v2.parser.SqrlHint;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.tools.Planner;

/**
 * A hint that has column names
 * Those get evaluated on the planner and the indexes added
 */
public abstract class ColumnNamesHint extends PlannerHint {

  private List<String> colNames;
  private List<Integer> colIndexes;

  protected ColumnNamesHint(ParsedObject<SqrlHint> source, Type type,
      List<String> columnNames) {
    super(source, type);
    this.colNames = columnNames;
    this.colIndexes = null;
  }

  public List<String> getColumnNames() {
    return colNames;
  }

  public List<Integer> getColumnIndexes() {
    return colIndexes;
  }

  /**
   * Updates the column names with the normalized names as they are defined
   * on the table
   * @param columnNames
   * @param columnIndexes
   */
  public void updateColumns(List<String> columnNames, List<Integer> columnIndexes) {
    this.colNames = columnNames;
    this.colIndexes = columnIndexes;
  }
}
