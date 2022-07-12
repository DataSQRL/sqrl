package ai.datasqrl.schema;

import ai.datasqrl.function.calcite.CalciteFunctionProxy;
import ai.datasqrl.parse.tree.ComparisonExpression;
import ai.datasqrl.parse.tree.ComparisonExpression.Operator;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Join.Type;
import ai.datasqrl.parse.tree.JoinOn;
import ai.datasqrl.parse.tree.Limit;
import ai.datasqrl.parse.tree.LongLiteral;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.Window;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedFunctionCall;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

@Getter
public class Relationship extends Field {

  private final Table fromTable;
  private final Table toTable; //todo rename to TargetTable
  private final JoinType joinType;
  private final Multiplicity multiplicity;

  private final Relation relation;
  private final Optional<OrderBy> orders;
  //Requires a transformation to limit cardinality to one
  private final Optional<Limit> limit;

  public Relationship(Name name, Table fromTable, Table toTable, JoinType joinType,
                      Multiplicity multiplicity, Relation relation, Optional<OrderBy> orders,
                      Optional<Limit> limit) {
    super(name);
    this.fromTable = fromTable;
    this.toTable = toTable;
    this.joinType = joinType;
    this.multiplicity = multiplicity;
    this.relation = relation;
    this.orders = orders;
    this.limit = limit;
  }

  public Relationship(Name name, Table fromTable, Table toTable, JoinType joinType,
                      Multiplicity multiplicity, Relation relation) {
    this(name, fromTable,toTable,joinType,multiplicity,relation, Optional.empty(), Optional.empty());
  }

  @Override
  public Name getId() {
    return name;
  }

  @Override
  public int getVersion() {
    return 0;
  }

  @Override
  public String toString() {
    return getId() + ": " + fromTable.getId() + " -> " + toTable.getId()
            + " [" + joinType + "," + multiplicity + "]";
  }

  public enum JoinType {
    PARENT, CHILD, JOIN
  }

  public enum Multiplicity {
    ZERO_ONE, ONE, MANY
  }
}