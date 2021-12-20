package ai.dataeng.sqml.analyzer2;

import static ai.dataeng.sqml.tree.name.NameCanonicalizer.AS_IS;

import ai.dataeng.sqml.analyzer2.TableManager.MaterializeTable;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.tree.AliasedRelation;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.GroupingElement;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeFormatter;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SimpleGroupBy;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

@AllArgsConstructor
public class Analyzer2 {

  Script script;
  TableEnvironment env;
  TableManager tableManager;
  private final ConversionError.Bundle<ConversionError> errors = new ConversionError.Bundle<>();
  ImportStub importStub;
  boolean devMode;

  public void analyze() {
    Analyzer2.Visitor visitor = new Analyzer2.Visitor();
    script.accept(visitor, null);
  }

  public static class Scope {

  }

  public class Visitor extends AstVisitor<Scope, Scope> {

    @Override
    public Scope visitNode(Node node, Scope context) {
      throw new RuntimeException(
          String.format("Could not process node %s : %s", node.getClass().getName(), node));
    }

    @Override
    public Scope visitScript(Script node, Scope context) {
      List<Node> statements = node.getStatements();
      for (Node statement : statements) {
        Preconditions.checkState(!(tableManager.isReadMode() && statement instanceof ImportDefinition),
            "Read mode set before imports");
        if (devMode && !(statement instanceof ImportDefinition)) {
          tableManager.setReadMode();
        }

        statement.accept(this, null);

      }

      return null;
    }

    @Override
    public Scope visitImportDefinition(ImportDefinition node, Scope scope) {
      importStub.importTable(node.getNamePath());

      return null;
    }

    @Override
    public Scope visitQueryAssignment(QueryAssignment node, Scope context) {
      String query = rewriteQuery(node, context, false);

      System.out.println(query);

      Table table = env.sqlQuery(query);
      SqrlEntity queryEntity = new SqrlEntity(node.getNamePath(), table);

      List<Name> pks = extractPrimaryKey(query);
      queryEntity.setPrimaryKey(pks);
      System.out.println("Primary keys:" + pks);


      String queryForMaterialize = tableManager.isReadMode()
          ? rewriteQuery(node, context, true)
          : query;

      tableManager.setTable(node.getNamePath(), queryEntity, queryForMaterialize);

      //Add relationship
      if (node.getNamePath().getPrefix().isPresent()) {
        SqrlEntity ent = tableManager.getTable(node.getNamePath().getPrefix().get());
        ent.addRelationship(node.getNamePath().getLast(), queryEntity);
      }

      return null;
    }

    private String rewriteQuery(QueryAssignment node, Scope context, boolean useViewDefinition) {
      Node n = node.getQuery();

      HasContextTable ctx = new HasContextTable();
      n.accept(ctx, null);
      if (ctx.isHasContext()) {
        SqrlEntity ent = tableManager.getTable(node.getNamePath().getPrefix().get());

        n = NodeTreeRewriter.rewriteWith(new DecontextualizerRewriter(),
            node.getQuery(),
            new RewriterContext(node.getNamePath().getPrefix().get(), ent));
        System.out.println(n.accept(new NodeFormatter(), null));
      }
      n = NodeTreeRewriter.rewriteWith(new TableNameRewriter(useViewDefinition), n, null);

      String query = n.accept(new NodeFormatter(), null);

      return query;
    }

    private List<Name> extractPrimaryKey(String query) {
      List<Name> keys = new ArrayList<>();

      //TODO: instead of reparsing, get it from temp table
      PlannerQueryOperation plannerQueryOperation = (PlannerQueryOperation)((TableEnvironmentImpl) env).getParser().parse(query).get(0);

      //The top node may not be an aggregate
      RelNode node = plannerQueryOperation.getCalciteTree();
      while (node instanceof Project) {
        node = ((Project)node).getInput();
      }

      if (node instanceof LogicalAggregate) { //TODO: This isn't always true, can we derive pk from tree at all?
        LogicalAggregate logicalAggregate = (LogicalAggregate) node;
        ImmutableBitSet groupingIndices = logicalAggregate.getGroupSet();
        RelRecordType inputFields = (RelRecordType)logicalAggregate.getInput().getRowType();
        //Get Data types of input project, then map them to the ouptut datatype
        List<RelDataTypeField> inputGroupingProjections = new ArrayList<>();

        for (Integer i : groupingIndices) {
          RelDataTypeField field = inputFields.getFieldList().get(i);
          inputGroupingProjections.add(field);
        }

        for (RelDataTypeField field : logicalAggregate.getRowType().getFieldList()) {
          if (inputGroupingProjections.contains(field)) {
            keys.add(Name.system(field.getName()));
          }
        }

      } else {
        //No grouping keys, use all output fields

        RelRecordType inputFields = (RelRecordType)plannerQueryOperation.getCalciteTree().getRowType();
        for (RelDataTypeField field : inputFields.getFieldList()) {
          //TODO: The table generated shouldn't use these as their primary keys but nested queries should
          // consider it to be its context keys.

          // keys.add(Name.system(field.getName()));
        }

      }
      return keys;
    }

    @Override
    public Scope visitExpressionAssignment(ExpressionAssignment node, Scope context) {
      SqrlEntity ent = tableManager.getTable(node.getNamePath().getPrefix().get());
      String expr = node.getExpression().accept(new NodeFormatter(), null);


      Table table = ent.getTable().addColumns(expr + " AS " + node.getNamePath().getLast().getDisplay());
      //Updates table ref
      ent.setTable(table);

      return null;
    }
  }




  @Value
  public class RewriterContext {
    NamePath currentContext;
    SqrlEntity currentContextEntity;
  }

  public class HasContextTable extends DefaultTraversalVisitor {
    boolean hasContext = false;
    @Override
    public Object visitTable(ai.dataeng.sqml.tree.Table node, Object context) {
      if (node.getNamePath().get(0).getDisplay().equalsIgnoreCase("@")) {
        hasContext = true;
      }

      return null;
    }

    public boolean isHasContext() {
      return hasContext;
    }
  }

  public class DecontextualizerRewriter extends NodeRewriter<RewriterContext> {

    @Override
    public Select rewriteSelect(Select node, RewriterContext context,
        NodeTreeRewriter<RewriterContext> treeRewriter) {
      List<SelectItem> selectItems = new ArrayList<>();
      selectItems.addAll(getContextKeys(context).stream().map(e->new SingleColumn(e)).collect(
          Collectors.toList()));
      selectItems.addAll(node.getSelectItems());

      return new Select(node.isDistinct(), selectItems);
    }

    public List<Identifier> getContextKeys(RewriterContext context) {
      return context.getCurrentContextEntity().getContextKey()
          .stream()
          .map(e->new Identifier(e.getDisplay()))
          .collect(Collectors.toList());
    }
    @Override
    public Node rewriteGroupBy(GroupBy node, RewriterContext context,
        NodeTreeRewriter<RewriterContext> treeRewriter) {
      return createGroupingExpressions(context, node.getGroupingElement().getExpressions());
    }

    public GroupBy createGroupingExpressions(RewriterContext context, List<Expression> additional) {
      List<Expression> grouping = new ArrayList();
      grouping.addAll(getContextKeys(context));
      grouping.addAll(additional);
      GroupingElement element = new SimpleGroupBy(grouping);
      return new GroupBy(element);
    }

    @Override
    public Node rewriteQuerySpecification(QuerySpecification node, RewriterContext context,
        NodeTreeRewriter<RewriterContext> treeRewriter) {
      Select select = rewriteSelect(node.getSelect(), context, treeRewriter);
      Relation from = NodeTreeRewriter.rewriteWith(this, node.getFrom(),
          context);
//      Optional<Expression> where = node.getWhere()
//          .map(value -> rewrite(value, context));
      Node groupBy = node.getGroupBy()
          .map(value -> rewriteGroupBy(value, context, treeRewriter))
          .orElseGet(() -> createGroupingExpressions(context, List.of()));


        return new QuerySpecification(
            node.getLocation(),
            select,
            from,
            node.getWhere(),
            Optional.of((GroupBy) groupBy),
            node.getHaving(),
            node.getOrderBy(),
            node.getLimit()
        );
      }

    //Todo: This should convert this to a ResolvedTable object
    @Override
    public Node rewriteTable(ai.dataeng.sqml.tree.Table node, RewriterContext context,
        NodeTreeRewriter treeRewriter) {
      SqrlEntity contextEntity = tableManager.getTable(context.getCurrentContext());
      System.out.println(contextEntity);
//      Name name = Name.of(tableManager.getTable(Name.system(node.getName().toString())).getTable().toString(), AS_IS);
      NamePath postfix = node.getNamePath().popFirst();

      return new ai.dataeng.sqml.tree.Table(context.getCurrentContext().resolve(postfix));
    }
  }


  @AllArgsConstructor
  public class TableNameRewriter extends NodeRewriter {
    boolean useViewDefinition;
    @Override
    public Node rewriteTable(ai.dataeng.sqml.tree.Table node, Object context,
        NodeTreeRewriter treeRewriter) {
      MaterializeTable tbl = tableManager.getMatTable(node.getNamePath());
      Preconditions.checkNotNull(tbl, node.getNamePath());
      String name;
      if (useViewDefinition) {
        name = tableManager.getCurrentName(tbl.getEntity());
      } else {
        name = tbl.getEntity().getTable().toString();
      }

      return new ai.dataeng.sqml.tree.Table(NamePath.of(Name.of(name, AS_IS)));
    }
  }

  public class FieldAndTableAliasRewriter extends NodeRewriter {

    @Override
    public Node rewriteTable(ai.dataeng.sqml.tree.Table node, Object context,
        NodeTreeRewriter treeRewriter) {
      return new AliasedRelation(
          new ai.dataeng.sqml.tree.Table(node.getNamePath()),
          new Identifier("test"));
    }

    @Override
    public Node rewriteAliasedRelation(AliasedRelation node, Object context,
        NodeTreeRewriter treeRewriter) {
      //skip aliased relations
      return node;
    }

    @Override
    public Node rewriteIdentifier(Identifier node, Object context, NodeTreeRewriter treeRewriter)  {
      SqrlField field = new SqrlField(node.getValue(), "test");

      return new Identifier(field.getQualifiedName());
    }
  }
}