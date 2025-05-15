package com.datasqrl.planner.analyzer;

import java.util.Collections;
import java.util.List;

import org.apache.flink.table.catalog.ObjectIdentifier;

import com.datasqrl.io.tables.TableType;

import lombok.AllArgsConstructor;
import lombok.Value;

public interface TableOrFunctionAnalysis extends AbstractAnalysis {

  ObjectIdentifier getIdentifier();

  List<String> getParameterNames();

  public TableType getType();

  default FullIdentifier getFullIdentifier() {
    return new FullIdentifier(getIdentifier(), getParameterNames());
  }

  /**
   * The base table on which this function is defined.
   * This means, that this table or function returns the same type as the base table.
   */
  TableAnalysis getBaseTable();

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
