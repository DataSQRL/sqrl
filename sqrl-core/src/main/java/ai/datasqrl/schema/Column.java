package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

@Getter
public class Column extends Field implements ShadowingContainer.IndexElement {

  /* Identity of the column in addition to name for shadowed columns which we need to keep
     on the table even though it is no longer referenceable since another column may depend on it
     (unlike relationships which can be ignored once they are shadowed and no longer referenceable)
   */
  private final int version;
  private final int index;

  private final boolean isPrimaryKey;
  private final boolean isParentPrimaryKey;

  //Column isn't visible to user but needed by system (for primary key or timestamp)
  private final boolean isVisible;

  public Column(Name name, int version, int index,
                boolean isPrimaryKey, boolean isParentPrimaryKey, boolean isVisible) {

    super(name);
    this.version = version;
    this.index = index;
    this.isVisible = isVisible;
    Preconditions.checkArgument(!isParentPrimaryKey || isPrimaryKey);
    this.isPrimaryKey = isPrimaryKey;
    this.isParentPrimaryKey = isParentPrimaryKey;
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

  @Override
  public boolean isVisible() {
    return isVisible;
  }

  @Override
  public String toString() {
    String s = getId() + " @" + index + ": ";
    if (isPrimaryKey) s += "pk ";
    if (isParentPrimaryKey) s += "ppk ";
    if (!isVisible) s += "hidden";
    return s;
  }

  @Value
  public static class LPDefinition {

    private final RexNode columnExpression;
    private final List<RelNode> leftJoins;
    private final int tableColumnWidth;
    private final Map<Integer, Column> columnPositionMap;


    public boolean isSimple() {
//      if (!leftJoins.isEmpty()) return false; //Column requires left-joins which makes it complex
//      //Else, check if the columns this column depends on are simple
//      for (Column col : columnPositionMap.values()) {
//        if (!col.isSimple()) return false;
//      }
      return true;
    }

  }

}
