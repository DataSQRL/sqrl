package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.constraint.Constraint;
import ai.datasqrl.schema.constraint.ConstraintHelper;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexNode;

@Getter
public class Column extends Field {

  /* Identity of the column in addition to name for shadowed columns which we need to keep
     on the table even though it is no longer referenceable since another column may depend on it
     (unlike relationships which can be ignored once they are shadowed and no longer referenceable)
   */
  private final int version;
  private final int index;
  private final RelDataType datatype;

  private final boolean isPrimaryKey;
  private final boolean isParentPrimaryKey;

  private final boolean nonNull;
  @NonNull private final List<Constraint> constraints;

  //Column isn't visible to user but needed by system (for primary key or timestamp)
  private final boolean isInternal;

  private final Optional<LPDefinition> definition = Optional.empty();

  public Column(Name name, int version, int index,
                RelDataType datatype,
                boolean isPrimaryKey, boolean isParentPrimaryKey,
                List<Constraint> constraints, boolean isInternal) {
    super(name);
    this.version = version;
    this.index = index;
    this.datatype = datatype;
    this.constraints = constraints;
    this.isInternal = isInternal;
    Preconditions.checkArgument(!isParentPrimaryKey || isPrimaryKey);
    this.isPrimaryKey = isPrimaryKey;
    this.isParentPrimaryKey = isParentPrimaryKey;
    this.nonNull = ConstraintHelper.isNonNull(constraints);
    Preconditions.checkArgument(!isPrimaryKey || nonNull);
    Preconditions.checkArgument(!isParentPrimaryKey || isPrimaryKey);
  }

  //Returns a calcite name for this column.
  public Name getId() {
    return getId(name,version);
  }

  public static Name getId(Name name, int version) {
    if (version==0) return name;
    else return name.suffix(Integer.toString(version));
  }

  public RelDataTypeField getRelDataTypeField() {
    return new RelDataTypeFieldImpl(name.getCanonical(),index,datatype);
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
        ", index =" + index +
        ", datatype=" + datatype +
        ", nonNull=" + nonNull +
        ", constraints=" + constraints +
        ", isPrimaryKey=" + isPrimaryKey +
        ", isParentPrimaryKey=" + isParentPrimaryKey +
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
