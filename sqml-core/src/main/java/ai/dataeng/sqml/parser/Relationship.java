package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.VersionedName;
import com.google.common.base.Preconditions;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 *
 * The last table referenced in a join declaration is the destination of the
 * join declaration. Join declarations maintain the same table and column
 * references which are resolved at definition time, and the table columns are
 * updated when referenced.
 *
 * Notes:
 *  - A join path in a table declaration has a different join treatment than in
 *    other identifiers. Table declarations are inner join while other declarations
 *    are outer joins.
 *  - Table's fields should only be appended so the relation can be updated in a deterministic
 *    way.
 */
@Getter
public class Relationship extends Field {
  public final Table toTable;
  public final Type type;

  public final Multiplicity multiplicity;

  @Setter
  private Map<Column, String> pkNameMapping;

  @Setter
  public SqlNode sqlNode;

  public int version = 0;

  public Relationship(
      Name name, Table fromTable, Table toTable, Type type, Multiplicity multiplicity,
      Map<Column, String> aliasMapping) {
    super(name, fromTable);
    this.toTable = toTable;
    this.type = type;
    this.multiplicity = multiplicity;
    this.pkNameMapping = aliasMapping;
  }

  public SqlNode getSqlNode() {
    Preconditions.checkNotNull(sqlNode, "Sql node should not be null");
    return sqlNode.clone(SqlParserPos.ZERO);
  }

  @Override
  public VersionedName getId() {
    return VersionedName.of(name, version);
  }

  @Override
  public int getVersion() {
    return version;
  }

  public enum Type {
    PARENT, CHILD, JOIN
  }

  public enum Multiplicity {
    ZERO_ONE, ONE, MANY
  }
}