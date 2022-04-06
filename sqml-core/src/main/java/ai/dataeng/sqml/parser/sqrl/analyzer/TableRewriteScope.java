package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Join.Type;
import ai.dataeng.sqml.tree.JoinCriteria;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.List;
import java.util.Optional;
import lombok.Value;

@Value
public class TableRewriteScope {

  Optional<Relation> current;

  /**
   * Additional
   * @return
   */
  public List<Field> getAdditionalFields() {
    return null;
  }

  public List<Identifier> getParentPrimaryKeys() {
    return List.of();
  }
}
