package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.tree.name.VersionedName;
import org.apache.calcite.sql.SqlNode;

public interface QualifiedNode {
  VersionedName getTableName();
  SqlNode getSqlNode();
}
