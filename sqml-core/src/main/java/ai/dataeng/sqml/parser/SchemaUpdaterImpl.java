package ai.dataeng.sqml.parser;

import static ai.dataeng.sqml.parser.macros.SqlNodeUtils.childParentJoin;
import static ai.dataeng.sqml.parser.macros.SqlNodeUtils.parentChildJoin;
import static ai.dataeng.sqml.tree.name.Name.PARENT_RELATIONSHIP;

import ai.dataeng.sqml.parser.Relationship.Multiplicity;
import ai.dataeng.sqml.parser.Relationship.Type;
import ai.dataeng.sqml.schema.Namespace;
import ai.dataeng.sqml.schema.SchemaUpdater;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.tree.name.VersionedName;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.tuple.Pair;

@Value
public class SchemaUpdaterImpl implements SchemaUpdater {
  Namespace namespace;

  @Override
  public VersionedName addTable(NamePath path, List<Field> fields) {
    Table destination = namespace.createTable(
        path.getLast(),
        path,
        false);
    fields.forEach(destination::addField);

    //Assignment to root
    if (path.getPrefix().isEmpty()) {
      List<Table> tables = new ArrayList<>();
      tables.add(destination);
      Dataset rootTable = new Dataset(Dataset.ROOT_NAMESPACE_NAME, tables);
      namespace.addDataset(rootTable);
    } else {
      Table source =
          namespace.lookup(path.getPrefix().orElseThrow())
              .orElseThrow(()->new RuntimeException(String.format("Could not find table on field %s", path)));
      Relationship fromField = new Relationship(path.getLast(),
          source, destination, Type.JOIN, Multiplicity.MANY, null);
      Pair<Map<Column, String>, SqlNode> child = parentChildJoin(fromField);

      fromField.setSqlNode(child.getRight());
      fromField.setPkNameMapping(child.getLeft());

      source.addField(fromField);

      Relationship toField = new Relationship(PARENT_RELATIONSHIP, destination, source, Type.PARENT,
          Multiplicity.ONE,
          null);
      Pair<Map<Column, String>, SqlNode> parent = childParentJoin(toField);
      toField.setSqlNode(parent.getRight());
      toField.setPkNameMapping(parent.getLeft());
      destination.addField(toField);
    }
    return new VersionedName(destination.getName().getCanonical(), destination.getName().getDisplay(), destination.getVersion());
  }

  @Override
  public VersionedName addJoinDeclaration(NamePath path, Relationship rel,
      Optional<Relationship> inverse) {
    Table table = namespace.lookup(path.getPrefix().get()).get();
    table.addField(rel);

    return new VersionedName(path.toString(), path.toString(), table.getVersion());
  }

  @Override
  public VersionedName addExpression(NamePath path, Field field) {
    Table table = namespace.lookup(path.getPrefix().get()).get();
    if (table.getField(field.getName()) != null) {
      ((Column)field).setVersion(table.getField(field.getName()).getVersion() + 1);
    }

    table.addField(field);

    return null;
  }
}
