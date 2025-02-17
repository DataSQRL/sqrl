package com.datasqrl.v2.analyzer;

import com.datasqrl.io.tables.TableType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.flink.table.catalog.ObjectIdentifier;

public interface TableOrFunctionAnalysis extends AbstractAnalysis {

  ObjectIdentifier getIdentifier();

  List<String> getParameterNames();

  public TableType getType();

  default FullIdentifier getFullIdentifier() {
    return new FullIdentifier(getIdentifier(), getParameterNames());
  }

  /**
   * A full identifier combines the object identifier
   * with the parameter list to unqiuely quality a table or function
   * within the catalog.
   *
   * Note, functions are not uniquely qualified by object identifier alone
   * since overloaded functions have the same identifier but a different argument signature.
   */
  @Value
  @AllArgsConstructor
  class FullIdentifier {
    ObjectIdentifier objectIdentifier;
    List<String> arguments;

    public FullIdentifier(ObjectIdentifier objectIdentifier) {
      this(objectIdentifier, Collections.EMPTY_LIST);
    }
  }

}
