package ai.datasqrl.validate;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.InlineJoin;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.SortItem;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.validate.scopes.InlineJoinScope;
import ai.datasqrl.validate.scopes.OrderByScope;
import ai.datasqrl.validate.scopes.QueryScope;
import ai.datasqrl.validate.scopes.ValidatorScope;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@AllArgsConstructor
public class QueryValidator extends AstVisitor<ValidatorScope, QueryScope> {

  public final Schema schema;

  @Override
  public ValidatorScope visitNode(Node node, QueryScope context) {
    throw new RuntimeException(
        String.format("Could not process node %s : %s", node.getClass().getName(), node));
  }

  @Override
  public ValidatorScope visitQuery(Query node, QueryScope scope) {
    RelationValidator relationValidator = new RelationValidator(schema);
    ValidatorScope queryBodyValidateScope = node.getQueryBody().accept(relationValidator, scope);

    //TODO: order & limit for Set operations
    return queryBodyValidateScope;
  }

  @Override
  public ValidatorScope visitInlineJoin(InlineJoin node, QueryScope scope) {
    RelationValidator relationValidator = new RelationValidator(schema);
    Map<Node, ValidatorScope> scopes = new HashMap<>();
    ValidatorScope relationScope = node.getRelation().accept(relationValidator, scope);
    scopes.put(node, relationScope);

    if (node.getOrderBy().isPresent()) {
      for (SortItem item : node.getOrderBy().get().getSortItems()) {
        ExpressionValidator expressionValidator = new ExpressionValidator();
        expressionValidator.validate(item.getSortKey(), scope);
        scopes.putAll(expressionValidator.getScopes());
      }
    }

    //TODO: order & limit for Set operations
    return new InlineJoinScope(scopes);
  }
}
