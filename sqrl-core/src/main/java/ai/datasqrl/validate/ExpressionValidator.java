package ai.datasqrl.validate;


import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.function.FunctionLookup;
import ai.datasqrl.function.SqrlFunction;
import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.DefaultTraversalVisitor;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.Join.Type;
import ai.datasqrl.parse.tree.JoinCriteria;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Field;
import ai.datasqrl.transform.Scope.ResolveResult;
import ai.datasqrl.validate.paths.PathUtil;
import ai.datasqrl.validate.scopes.ExpressionScope;
import ai.datasqrl.validate.scopes.FunctionCallScope;
import ai.datasqrl.validate.scopes.IdentifierScope;
import ai.datasqrl.validate.scopes.QueryScope;
import ai.datasqrl.validate.scopes.ValidatorScope;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.Value;

@Getter
public class ExpressionValidator {
  Map<Node, ValidatorScope> scopes = new HashMap<>();
  ErrorCollector errorCollector = ErrorCollector.root();
  public ExpressionValidator() {
  }

  public ExpressionScope validate(Expression node, QueryScope scope) {
    Visitor visitor = new Visitor();
    return node.accept(visitor, scope);
  }

  class Visitor extends DefaultTraversalVisitor<ExpressionScope, QueryScope> {

    /**
     * Validates and resolves function, validates to-many aggregations function
     */
    @Override
    public ExpressionScope visitFunctionCall(FunctionCall node, QueryScope scope) {
//      SqrlFunction function = scope.getNamespace().resolveFunction(node.getNamePath());
      FunctionLookup functionLookup = new FunctionLookup();
      SqrlFunction function = functionLookup.lookup(node.getNamePath());

      FunctionCallScope functionCallScope = new FunctionCallScope(function, function.isAggregate());
      scopes.put(node, functionCallScope);

      Map<Node, ValidatorScope> argumentScopes = new HashMap<>();
      for (Expression argument : node.getArguments()) {
        ExpressionValidator expressionValidator = new ExpressionValidator();
        expressionValidator.validate(argument, scope);
        argumentScopes.putAll(expressionValidator.getScopes());
        scopes.putAll(expressionValidator.getScopes());
      }

      /*
       * Validates if an argument can contain a to-many path
       */
      for (ValidatorScope argScope : argumentScopes.values()) {
        if (argScope instanceof IdentifierScope
            && PathUtil.isToMany((IdentifierScope) argScope)) {
          if (!function.isAggregate() || node.getArguments().size() != 1) {
            errorCollector.fatal("To-many error");
          }
        }
      }

      return functionCallScope;
    }

    @Override
    public ExpressionScope visitIdentifier(Identifier node, QueryScope scope) {
      List<ResolveResult> results = scope.resolveFirst(node.getNamePath());

      Preconditions.checkState(results.size() == 1,
          "Could not resolve field (ambiguous or non-existent: " + node + " : " + results + ")");

      ResolveResult result = results.get(0);
      NamePath qualifiedName = scope.qualify(node.getNamePath());
      List<Field> fieldPath = result.getTable().walkFields(result.getRemaining().get());

      IdentifierScope identifierScope = new IdentifierScope(result.getAlias(), qualifiedName, fieldPath, result);
      scopes.put(node, identifierScope);
      return identifierScope;
    }
  }

  @Value
  public static class JoinResult {
    Type type;
    Relation relation;
    Optional<JoinCriteria> criteria;
  }
}
