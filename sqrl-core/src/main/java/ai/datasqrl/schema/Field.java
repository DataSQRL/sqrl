package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.analyzer.Analysis.ResolvedNamePath;
import java.util.Optional;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public abstract class Field implements ShadowingContainer.Element {

  protected final Name name;

  protected Field(Name name) {
    this.name = name;
  }

  public abstract Name getId();

}
