package ai.dataeng.sqml.analyzer2;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.table.api.Table;

@Getter
@Setter
public class SqrlTable {
  private static final AtomicInteger versionInc = new AtomicInteger();

  private final NamePath namePath;
  private final int version;
  private final Table table;
  private final String sql;
  private final Map<Name, SqrlTable> relationships = new HashMap<>();
  private final List<Name> primaryKey;

  //Resolved schema
  //Calcite logical plan

  public SqrlTable(NamePath namePath, Table table, String sql,
      List<Name> pks) {
    this.namePath = namePath;
    this.table = table;
    this.sql = sql;
    this.primaryKey = pks;
    this.version = versionInc.incrementAndGet();
  }

  /**
   * Adds a column to the table, returns new table
   * @param name
   * @param sql
   * @return
   */
  public SqrlTable addColumn(Name name, String sql) {
    //TODO: Parse expression to -> SELECT sql FROM @;
    //Do query rewriting, if agg do a join, otherwise combine project?

    Table flinkTable = table.addColumns(sql + " AS " +name.getDisplay());

    return createTable(flinkTable);
  }

  private SqrlTable createTable(Table flinkTable) {
    return new SqrlTable(this.namePath, flinkTable, null, this.primaryKey);
  }


  public SqrlTable addRelColumn(Name name, SqrlTable table) {
    this.relationships.put(name, table);
    return table;
  }

  public List<Name> getPrimaryKey() {
    return primaryKey;
  }

//
//  public List<Name> getContextKey() {
//    List<Name> contextKeys = new ArrayList<>();
//    if (parent != null) {
//      contextKeys.addAll(parent.getContextKey());
//    }
//    contextKeys.addAll(primaryKey);
//    return contextKeys;
//  }

  //TODO: Fix
  public List<Name> getContextKeyWoPk() {
    return List.of();
//    List<Name> contextKeys = new ArrayList<>();
//    if (parent != null) {
//      contextKeys.addAll(parent.getContextKey());
//    }
//    return contextKeys;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SqrlTable table = (SqrlTable) o;
    return Objects.equals(namePath, table.namePath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namePath);
  }
}
