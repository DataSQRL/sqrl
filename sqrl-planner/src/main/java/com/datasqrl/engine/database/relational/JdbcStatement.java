package com.datasqrl.engine.database.relational;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;

@Value
public class JdbcStatement {

  public enum Type {TABLE, VIEW, QUERY, INDEX, EXTENSION}

  /**
   * The name of the table/view/query/index
   */
  String name;
  /**
   * The type of statement
   */
  Type type;
  /**
   * The SQL representation of this statement
   */
  String sql;
  /**
   * The datatype for table, view, and query.
   * Is null for other types.
   */
  @JsonIgnore
  RelDataType dataType;
  /**
   * The datatype converted to a list of Field.
   * It's null if there is no dataType.
   */
  List<Field> fields;

  public JdbcStatement(String name, Type type, String sql) {
    this(name,type,sql,null, null);
  }

  public JdbcStatement(String name, Type type, String sql, RelDataType dataType, List<Field> fields) {
    this.name = name;
    this.type = type;
    this.sql = sql;
    this.dataType = dataType;
    this.fields = fields;
  }

  @Value
  public static class Field {
    String name;
    String type;
    boolean isNullable;
  }


}
