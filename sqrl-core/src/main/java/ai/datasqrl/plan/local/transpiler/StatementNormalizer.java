package ai.datasqrl.plan.local.transpiler;

import static ai.datasqrl.parse.util.SqrlNodeUtil.hasOneUnnamedColumn;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.CreateSubscription;
import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.JoinAssignment;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.QueryAssignment;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinDeclarationNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.QuerySpecNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import ai.datasqrl.plan.local.transpiler.transforms.DistinctTransform;
import ai.datasqrl.plan.local.transpiler.transforms.ExpressionToQueryTransformer;
import ai.datasqrl.plan.local.transpiler.transforms.RejoinExprTransform;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import com.google.common.base.Preconditions;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Converts a statement into its intermediate form.
 */
@Slf4j
@AllArgsConstructor
@Getter
public class StatementNormalizer {

  private Schema schema;

  private final RelationNormalizer relationNormalizer = new RelationNormalizer();

  protected final ErrorCollector errors = ErrorCollector.root();

  public Node normalize(Node node) {
    Visitor visitor = new Visitor();
    return node.accept(visitor, null);
  }

  public class Visitor extends AstVisitor<Node, Void> {

    @Override
    public Node visitNode(Node node, Void context) {
      throw new RuntimeException(
          String.format("Could not process node %s : %s", node.getClass().getName(), node));
    }

    @Override
    public Node visitQueryAssignment(QueryAssignment queryAssignment, Void context) {
      NamePath namePath = queryAssignment.getNamePath();
      Query query = queryAssignment.getQuery();

      boolean isExpression = hasOneUnnamedColumn(queryAssignment.getQuery());
      RelationScope scope = createScope(namePath, isExpression);
      Node norm = relationNormalizer.normalize(query, scope);

      return isExpression
          ? RejoinExprTransform.transform((QuerySpecNorm) norm, scope.getContextTable().get())
          : norm;
    }

    @Override
    public Node visitExpressionAssignment(ExpressionAssignment assignment, Void context) {
      NamePath namePath = assignment.getNamePath();

      ExpressionToQueryTransformer toQueryTransformer = new ExpressionToQueryTransformer();
      Query query = toQueryTransformer.transform(assignment.getExpression());

      RelationScope scope = createScope(namePath, true);
      Node norm =  relationNormalizer.normalize(query, scope);

      return RejoinExprTransform.transform((QuerySpecNorm)norm, scope.getContextTable().get());
    }

    @Override
    public Node visitCreateSubscription(CreateSubscription node, Void context) {
      RelationScope scope = createScope(node.getNamePath());
      return relationNormalizer.normalize(node.getQuery(), scope);
    }

    /**
     * Validates tables and column names.
     */
    @SneakyThrows
    @Override
    public Node visitDistinctAssignment(DistinctAssignment node, Void context) {
      Preconditions.checkState(node.getNamePath().getLength() == 1,
          "Distinct node must be on root (tbd expand)");
      Optional<Table> tableOpt = schema.getVisibleByName(node.getTable());
      Preconditions.checkState(tableOpt.isPresent(),
          "Table could not be found: " + node.getTable());

      DistinctTransform distinctTransform = new DistinctTransform();
      Query spec = distinctTransform.transform(node, tableOpt.get());

      RelationScope scope = createScope(node.getNamePath());
      return relationNormalizer.normalize(spec, scope);
    }

    @Override
    public Node visitImportDefinition(ImportDefinition node, Void context) {
      return null;
    }

    @Override
    public Node visitJoinAssignment(JoinAssignment node, Void context) {
      RelationScope scope = createScope(node.getNamePath());

      RelationNorm joinNorm = node.getJoinDeclaration().getRelation().accept(relationNormalizer, scope);

      Optional<OrderBy> orderBy = relationNormalizer.rewriteOrderBy(node.getJoinDeclaration().getOrderBy(), scope);

      return new JoinDeclarationNorm(node.getJoinDeclaration().getLocation(),
          joinNorm,
          orderBy,
          node.getJoinDeclaration().getLimit(),
          node.getJoinDeclaration().getInverse());
    }

    private Optional<Table> getContext(NamePath namePath) {
      if (namePath.getLength() == 0) {
        return Optional.empty();
      }
      Table table = schema.getVisibleByName(namePath.getFirst()).get();
      return table.walk(namePath.popFirst());
    }

    private RelationScope createScope(NamePath namePath) {
      return createScope(namePath, false);
    }

    private RelationScope createScope(NamePath namePath, boolean isExpression) {
      Optional<Table> contextTable = getContext(namePath.popLast());
      return new RelationScope(schema, contextTable, isExpression, namePath.getLast());
    }
  }
}
