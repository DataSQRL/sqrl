package ai.dataeng.sqml.physical;

import ai.dataeng.sqml.analyzer.Field;
import ai.dataeng.sqml.function.SqmlFunction;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.Window;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor
public class H2NestedExpressionRewriter extends AstVisitor<Node, RewriteScope> {
  private Metadata metadata;

  @Override
  protected Node visitFunctionCall(FunctionCall node, RewriteScope scope) {
    //Todo: need to fully qualify functions

    //Todo move function resolution to scope
    Optional<SqmlFunction> function = metadata.getFunctionProvider().resolve(node.getName());
    if (function.isEmpty()) {
      throw new RuntimeException(String.format("Could not find function %s", node.getName()));
    }
    Optional<Window> window;
    if (function.get().isAggregation()) {
      window = Optional.of(new Window(toIdentifierList(scope.getPrimaryKeys()), Optional.empty()));
    } else {
      window = Optional.empty();
    }

    //Todo: Arguments
    List<Expression> arguments;
    if (function.get().isAggregation()) {
      //Note: h2 requires at least 1 argument in an agg function, so use first primary key
      arguments = List.of(toIdentifierList(scope.getPrimaryKeys()).get(0));
    } else {
      arguments = List.of();
    }

    FunctionCall functionCall = new FunctionCall(Optional.empty(), node.getName(),
        arguments,
        node.isDistinct(), window);

    return new SingleColumn(functionCall, new Identifier(scope.getName()));
  }

  private List<Expression> toIdentifierList(List<Field> fields) {
    return fields.stream()
        .map(e->new Identifier(e.getName().get()))
        .collect(Collectors.toList());
  }
}

@Value
class RewriteScope {
  private final List<Field> primaryKeys;
  private String name;
}
