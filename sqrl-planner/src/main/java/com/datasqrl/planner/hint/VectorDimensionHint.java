package com.datasqrl.planner.hint;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;
import com.datasqrl.planner.parser.StatementParserException;
import com.google.auto.service.AutoService;
import java.util.List;
import lombok.Getter;

/**
 * Assigns a partition key to a table that is persisted into a data system engine.
 */
public class VectorDimensionHint extends ColumnNamesHint implements DataTypeHint {

  public static final String HINT_NAME = "vector_dim";

  @Getter
  private final int dimensions;

  protected VectorDimensionHint(ParsedObject<SqrlHint> source, String column, int dimension) {
    super(source, Type.DAG, List.of(column));
    this.dimensions = dimension;
  }

  @Override
  public String getColumnName() {
    return super.getColumnNames().get(0);
  }

  @Override
  public int getColumnIndex() {
    return super.getColumnIndexes().get(0);
  }

  @AutoService(Factory.class)
  public static class VectorDimensionFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      if (source.get().getOptions().size()!=2) {
        throw new StatementParserException(ErrorLabel.GENERIC, source.getFileLocation(),
            "Vector dimension hint requires two arguments: the name of the vector column and the number of dimensions.");
      }
      String dimensionsString = source.get().getOptions().get(1);
      int dimensions;
      try {
        dimensions = Integer.parseInt(dimensionsString);
      } catch (NumberFormatException e) {
        throw new StatementParserException(ErrorLabel.GENERIC, source.getFileLocation(),
            "Vector dimension must be a valid number: %s (%s).", dimensionsString, e.getMessage());
      }
      if (dimensions <= 0) {
        throw new StatementParserException(ErrorLabel.GENERIC, source.getFileLocation(),
            "Vector dimension must be a positive number: %s.", dimensionsString);
      }
      return new VectorDimensionHint(source, source.get().getOptions().get(0), dimensions);
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }

}
