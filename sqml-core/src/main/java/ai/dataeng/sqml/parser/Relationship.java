package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.JoinOn;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.VersionedName;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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

  private final Table table;
  public final Table toTable;
  public final Type type;
  @Setter
  public Node node;

  public final Multiplicity multiplicity;

  public int version = 0;
  private Name alias;

  public Relationship(
      Name name, Table fromTable, Table toTable, Type type, Multiplicity multiplicity) {
    super(name);
    this.table = fromTable;
    this.toTable = toTable;
    this.type = type;
    this.multiplicity = multiplicity;
  }


  @Override
  public VersionedName getId() {
    return VersionedName.of(name, version);
  }

  @Override
  public int getVersion() {
    return version;
  }

  public Node getNode() {
    Query query = (Query) node;
    QuerySpecification spec = (QuerySpecification)query.getQueryBody();
    spec.setSelect(new Select(Optional.empty(), false, refreshSelect(spec.getSelect().getSelectItems())));
    return node;
  }

  private List<SelectItem> refreshSelect(
      List<SelectItem> groupingColumns) {
    List<SelectItem> selectItems = new ArrayList<>();
    selectItems.addAll(groupingColumns);
    for (Field field : this.toTable.getFields().getElements()) {
      if (field instanceof Relationship) continue;
      selectItems.add(new SingleColumn(new Identifier(Optional.empty(), this.alias.toNamePath().concat(((Column)field).getId()))));
    }
    return selectItems;
  }

  public void setAlias(Name alias) {
    this.alias = alias;
  }

  public enum Type {
    PARENT, CHILD, JOIN
  }

  public enum Multiplicity {
    ZERO_ONE, ONE, MANY
  }
}