package com.datasqrl.v2.hint;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.calcite.rel.type.RelDataTypeField;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.v2.parser.ParsedObject;
import com.datasqrl.v2.parser.SqrlHint;
import com.datasqrl.v2.parser.StatementParserException;

/**
 * A hint that has column names
 * Those get validated by {@link com.datasqrl.v2.analyzer.SQRLLogicalPlanAnalyzer} and indexes
 * are added. Hence, it can be assumed that the hints have resolved indexes after planning.
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
  private void updateColumns(List<String> columnNames, List<Integer> columnIndexes) {
    this.colNames = columnNames;
    this.colIndexes = columnIndexes;
  }

  public void validateAndUpdate(Function<String, RelDataTypeField> fieldByIndex) {
    //Validate column names in hints and map to indexes
    List<String> colNames = new ArrayList<>();
    List<Integer> colIndexes = new ArrayList<>();
    for (String colName : getColumnNames()) {
      var field = fieldByIndex.apply(colName);
      if (field == null) {
        throw new StatementParserException(ErrorLabel.GENERIC, getSource().getFileLocation(),
            "%s hint reference column [%s] that does not exist in table", getName(), colName);
      }
      colNames.add(field.getName());
      colIndexes.add(field.getIndex());
    }
    updateColumns(colNames, colIndexes);
  }
}
