package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import com.google.common.base.Preconditions;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;

@Getter
public class Relationship extends Field {

  private final SQRLTable fromTable;
  private final SQRLTable toTable;
  private final JoinType joinType;
  private final Multiplicity multiplicity;
  private final SqlNode node;

  public Relationship(Name name, int version, SQRLTable fromTable, SQRLTable toTable, JoinType joinType,
                      Multiplicity multiplicity, SqlNode node) {
    super(name, version);
    this.fromTable = fromTable;
    this.toTable = toTable;
    Preconditions.checkNotNull(toTable);
    this.joinType = joinType;
    this.multiplicity = multiplicity;
    this.node = node;
  }

  @Override
  public String toString() {
    return fromTable.getName() + " -> " + toTable.getName()
            + " [" + joinType + "," + multiplicity + "]";
  }

  @Override
  public FieldKind getKind() {
    return FieldKind.RELATIONSHIP;
  }

  public enum JoinType {
    PARENT, CHILD, JOIN
  }

  public enum Multiplicity {
    ZERO_ONE, ONE, MANY
  }
}