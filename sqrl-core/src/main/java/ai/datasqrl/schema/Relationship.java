package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.SelectItem;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.VersionedName;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;

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
//    Query query = (Query) node;
//    QuerySpecification spec = (QuerySpecification)query.getQueryBody();
//    spec.setSelect(new Select(Optional.empty(), false, spec.getSelect().getSelectItems()));
    return node;
  }

  private List<SelectItem> refreshSelect() {
    List<SelectItem> selectItems = new ArrayList<>();
    Query query = new Query(((Query)this.node).getQueryBody(), Optional.empty(), Optional.empty());
    Table table = new TableFactory().create(query);
    for (Field field : table.getFields()) {
      Identifier identifier = new Identifier(Optional.empty(), field.getName().toNamePath());
      identifier.setResolved(field);
      selectItems.add(new SingleColumn(Optional.empty(), identifier, Optional.empty()));
    }

//
//    for (Field field : this.toTable.getFields().getElements()) {
//      if (field instanceof Relationship) continue;
//      Identifier identifier = new Identifier(Optional.empty(), field.getId().toNamePath());
//      identifier.setResolved(field);
//      selectItems.add(new SingleColumn(identifier));
//    }
//    for (int i = 0; i < this.table.getPrimaryKeys().size(); i++) {
//      Column field = this.table.getPrimaryKeys().get(i);
//      Identifier key = new Identifier(Optional.empty(), field.getId().toNamePath());
//      key.setResolved(field);
//      selectItems.add(new SingleColumn(key, Optional.of(new Identifier(Optional.empty(), Name.system("_pk"+(i+ 5)).toNamePath()))));
//    }

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