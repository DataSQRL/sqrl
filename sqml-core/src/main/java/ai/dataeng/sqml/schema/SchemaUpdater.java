package ai.dataeng.sqml.schema;

import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.tree.name.VersionedName;
import java.util.List;
import java.util.Optional;

/**
 * Updates the schema w/ parsed results. Maintains the hierarchy & resolves fields from sql nodes.
 */
public interface SchemaUpdater {
  public VersionedName addTable(NamePath path, List<Field> fields);
  public VersionedName addJoinDeclaration(NamePath path, Relationship rel, Optional<Relationship> inverse);
  public VersionedName addExpression(NamePath path, Field field);
}
