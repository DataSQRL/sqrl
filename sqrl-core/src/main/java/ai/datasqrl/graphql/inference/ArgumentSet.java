package ai.datasqrl.graphql.inference;

import ai.datasqrl.graphql.server.Model.Argument;
import ai.datasqrl.graphql.server.Model.ArgumentPgParameter;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;

@AllArgsConstructor
@Getter
public class ArgumentSet {

  RelNode relNode;
  Set<Argument> argumentHandlers;
  List<ArgumentPgParameter> argumentParameters;
  @Setter
  boolean limitOffsetFlag;
}