package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.tree.name.Name;
import java.util.Collection;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.math3.util.Pair;

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
  private Relationship inverse;
  @Setter
  public RelNode node;

  @Setter
  public SqlNode sqlNode;
  private List<Pair<String, String>> keys;

  public Relationship(
      Name name, Table fromTable, Table toTable, Type type, Multiplicity multiplicity,
      Relationship inverse,
      RelNode relNode) {
    super(name, fromTable);
    this.toTable = toTable;
    this.type = type;
    this.multiplicity = multiplicity;
    this.inverse = inverse;
    this.node = relNode;
  }


  public SqlNode getCondition() {
    SqlJoin join = (SqlJoin)sqlNode;
    return join.getCondition();
  }

  public SqlNode getRight() {
    SqlJoin join = (SqlJoin)sqlNode;
    return join.getRight();
  }

  @Override
  public Field copy() {
    throw new RuntimeException("Cannot copy relationship");
  }

  @Override
  public String getId() {
    return name.getCanonical() + LogicalPlanImpl.ID_DELIMITER + Integer.toHexString(0);
  }

  @Override
  public int getVersion() {
    return 0;
  }

  public void setJoinKeys(List<Pair<String, String>> keys) {
    this.keys = keys;
  }

  public enum Type {
    PARENT, CHILD, JOIN
  }

  public enum Multiplicity {
    ZERO_ONE, ONE, MANY
  }
}