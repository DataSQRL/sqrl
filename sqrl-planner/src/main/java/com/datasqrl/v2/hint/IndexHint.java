package com.datasqrl.v2.hint;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.function.IndexType;
import com.datasqrl.v2.parser.ParsedObject;
import com.datasqrl.v2.parser.SqrlHint;
import com.datasqrl.v2.parser.StatementParserException;
import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Optional;
import lombok.Getter;

/**
 * Explicitly assign an index to a table that's persisted to a database engine.
 * Overwrites the automatically determined index structures.
 */
@Getter
public class IndexHint extends ColumnNamesHint {

  public static final String HINT_NAME = "index";

  private final IndexType indexType;

  protected IndexHint(ParsedObject<SqrlHint> source, IndexType indexType, List<String> columnsNames) {
    super(source, Type.DAG, columnsNames);
    this.indexType = indexType;
  }

  @AutoService(Factory.class)
  public static class IndexHintFactory implements Factory {

    @Override
    public PlannerHint create(ParsedObject<SqrlHint> source) {
      List<String> arguments = source.get().getOptions();
      if (arguments==null || arguments.isEmpty()) return new IndexHint(source, null, List.of()); //no hint
      if (arguments.size()<= 1) throw new StatementParserException(ErrorLabel.GENERIC, source.getFileLocation(),
          "Index hint requires at least two arguments: the name of the index type and at least one column.");
      Optional<IndexType> optIndex = IndexType.fromName(arguments.get(0));
      if (optIndex.isEmpty()) throw new StatementParserException(ErrorLabel.GENERIC, source.getFileLocation(),  "Unknown index type: %s", arguments.get(0));
      return new IndexHint(source, optIndex.get(), arguments.subList(1, arguments.size()));
    }

    @Override
    public String getName() {
      return HINT_NAME;
    }
  }

}
