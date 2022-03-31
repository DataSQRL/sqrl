package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.tree.Join.Type;
import ai.dataeng.sqml.tree.JoinCriteria;
import ai.dataeng.sqml.tree.Relation;
import java.util.Optional;
import lombok.Value;

@Value
public class TableRewriteScope {

  Optional<Relation> current;

  public void setPushdownCondition(Type type, Optional<JoinCriteria> criteria) {

  }
}
