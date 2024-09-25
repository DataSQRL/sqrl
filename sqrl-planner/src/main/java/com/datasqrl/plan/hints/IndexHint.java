package com.datasqrl.plan.hints;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.IndexType;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import lombok.Value;

@Value
public class IndexHint implements OptimizerHint {

  public static final String PARTITION_KEY_HINT = "partition_key";
  public static final String INDEX_HINT = "index";

  public static final IndexHint NONE = new IndexHint(null, List.of());

  IndexType indexType;
  List<String> columnNames;

  public static IndexHint of(String hintName, List<String> arguments, ErrorCollector errors) {
    if (arguments==null || arguments.isEmpty()) return NONE;
    List<String> columnNames;
    IndexType indexType;
    if (hintName.equalsIgnoreCase(PARTITION_KEY_HINT)) {
      indexType = IndexType.PBTREE;
      columnNames = arguments;
    } else if (hintName.equalsIgnoreCase(INDEX_HINT)) {
      errors.checkFatal(arguments.size() > 1, "Index hint requires at least two arguments: the name of the index type and at least one column.");
      Optional<IndexType> optIndex = IndexType.fromName(arguments.get(0));
      errors.checkFatal(optIndex.isPresent(), "Unknown index type: %s", arguments.get(0));
      indexType = optIndex.get();
      columnNames = arguments.subList(1, arguments.size());
    } else {
      throw new IllegalArgumentException("Unknown hint: " + hintName);
    }
    errors.checkFatal(!columnNames.isEmpty(), "Index hint requires at least one column.");
    return new IndexHint(indexType, columnNames);
  }

  public IndexType getIndexType() {
    Preconditions.checkArgument(indexType!=null, "Not an index");
    return indexType;
  }

  public boolean isValid() {
    return indexType!=null;
  }


}
