package ai.datasqrl.graphql.inference;

import ai.datasqrl.graphql.server.Model.Argument;
import java.util.Set;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class ArgumentSet {

  RelNode relNode;
  Set<Argument> argumentHandlers;
  boolean limitOffsetFlag;
}