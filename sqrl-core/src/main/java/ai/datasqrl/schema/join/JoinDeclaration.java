package ai.datasqrl.schema.join;

import org.apache.calcite.sql.SqlNode;

public interface JoinDeclaration {

  SqlNode getQuery();
}
