package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.constraint.Constraint;
import ai.datasqrl.schema.constraint.ConstraintHelper;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

@Getter
public class Column extends Field {

  /* Identity of the column in addition to name for shadowed columns which we need to keep
     on the table even though it is no longer referenceable since another column may depend on it
     (unlike relationships which can be ignored once they are shadowed and no longer referenceable)
   */
  private final int version;

  private final boolean isPrimaryKey;
  private final boolean isParentPrimaryKey;
  private final boolean isTimestamp;

  private final boolean nonNull;
  private final List<Constraint> constraints;
  private final RelDataTypeField relDataTypeField;

  //System information
  private final boolean isInternal;

  private final Optional<LPDefinition> definition = Optional.empty();

  public Column(Name name, int version,
                RelDataTypeField relDataTypeField, List<Constraint> constraints,
                boolean isInternal, boolean isPrimaryKey,
                boolean isParentPrimaryKey, boolean isTimestamp) {
    super(name);
    this.version = version;
    this.relDataTypeField = relDataTypeField;
    this.constraints = constraints;
    this.isInternal = isInternal;
    Preconditions.checkArgument(!isParentPrimaryKey || isPrimaryKey);
    this.isPrimaryKey = isPrimaryKey;
    this.isParentPrimaryKey = isParentPrimaryKey;
    this.isTimestamp = isTimestamp;
    this.nonNull = ConstraintHelper.isNonNull(constraints);
  }

  //Returns a calcite name for this column.
  public Name getId() {
    return Name.system(relDataTypeField.getName());
  }

  @Override
  public boolean isVisible() {
    return !isInternal;
  }

  /**
   * Whether this column is a simple projection column
   * @return
   */
  public boolean isSimple() {
    if (definition.isEmpty()) return true; //Column is defined by original query
    LPDefinition def = definition.get();
    if (!def.leftJoins.isEmpty()) return false; //Column requires left-joins which makes it complex
    //Else, check if the columns this column depends on are simple
    for (Column col : def.columnPositionMap.values()) {
      if (!col.isSimple()) return false;
    }
    return true;
  }

  public boolean isComplex() {
    return !isSimple();
  }

  @Override
  public String toString() {
    return "Column{" +
        "version=" + version +
        ", nonNull=" + nonNull +
        ", constraints=" + constraints +
        ", isPrimaryKey=" + isPrimaryKey +
        ", isParentPrimaryKey=" + isParentPrimaryKey +
        ", isTimestamp =" + isTimestamp +
        ", relDataTypeField=" + relDataTypeField +
        ", isInternal=" + isInternal +
        ", name=" + name +
        '}';
  }

  @Value
  public static class LPDefinition {

    private final RexNode columnExpression;
    private final List<RelNode> leftJoins;
    private final int tableColumnWidth;
    private final Map<Integer, Column> columnPositionMap;

  }

}
