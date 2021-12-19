package ai.dataeng.sqml.analyzer2;

import static ai.dataeng.sqml.tree.name.NameCanonicalizer.AS_IS;
import static ai.dataeng.sqml.tree.name.NameCanonicalizer.LOWERCASE_ENGLISH;

import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.tree.AliasedRelation;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.tree.Expression;
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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.Operation;

@AllArgsConstructor
public class Analyzer2 {

  Script script;
  TableEnvironment env;
  TableManager tableManager;
  private final ConversionError.Bundle<ConversionError> errors = new ConversionError.Bundle<>();

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
        statement.accept(this, null);
      }

      return null;
    }

    @Override
    public Scope visitImportDefinition(ImportDefinition node, Scope scope) {
      final String ordersPath = "file:///Users/henneberger/Projects/sqml-official/sqml-examples/retail/ecommerce-data/orders.json";

      TableResult tableResult = env.executeSql(
          "CREATE TABLE Orders ("
              + "  id BIGINT,"
              + "  customerid INT,"
              + "  `time` INT,"
              + "  `proctime` AS PROCTIME(),"
//              + "  `uuid` AS UUID(),"
              + "  entries ARRAY<ROW<productid INT, quantity INT, unit_price INT, discount INT>>"
              + ") WITH ("
              + "  'connector' = 'filesystem',"
              + "  'path' = '"
              + ordersPath
              + "',"
              + "  'format' = 'json'"
              + ")");

      Table orders = env.sqlQuery(
          "SELECT id, customerid, `time`, `proctime`, UUID() as `uuid`, entries FROM Orders");

      TableResult reslt = env.executeSql(
          "CREATE VIEW Orders2 AS SELECT id, customerid, `time`, `proctime`, UUID() as `uuid`, entries FROM Orders");
      SqrlEntity decoratedOrders = new SqrlEntity(orders);
      decoratedOrders.setPrimaryKey(List.of(Name.of("id", LOWERCASE_ENGLISH)));
      tableManager.getTables().put(Name.of("Orders", LOWERCASE_ENGLISH).toNamePath(), decoratedOrders);

      //FK are id mapping to id
      Table entries = env.sqlQuery(
          "SELECT o.id AS `id`, o.proctime, e.* FROM Orders o, UNNEST(o.`entries`) e");
      SqrlEntity decoratedEntries = new SqrlEntity(entries);

      decoratedOrders.addRelationship(Name.system("entries"), decoratedEntries);
      tableManager.getTables().put(NamePath.of(
          Name.of("Orders", LOWERCASE_ENGLISH),
          Name.of("entries", LOWERCASE_ENGLISH)
      ), decoratedEntries);

      return null;
    }

    @Override
    public Scope visitQueryAssignment(QueryAssignment node, Scope context) {
//        Node rewritten = NodeTreeRewriter.rewriteWith(new FieldAndTableAliasRewriter(), node.getQuery(), null);
        //Must be after aliasing
      SqrlEntity ent = tableManager.getTables().get(node.getNamePath().getPrefix().get());
      Node n = node.getQuery();
      HasContextTable ctx = new HasContextTable();
      n.accept(ctx, null);
      if (ctx.isHasContext()) {
        n = NodeTreeRewriter.rewriteWith(new DecontextualizerRewriter(),
            node.getQuery(),
            new RewriterContext(node.getNamePath().getPrefix().get(), ent));
        System.out.println(n.accept(new NodeFormatter(), null));
      }
      n = NodeTreeRewriter.rewriteWith(new TableNameRewriter(), n, null);

      String query = n.accept(new NodeFormatter(), null);
      System.out.println(query);

      //Nested tables
//      Node node2 = NodeTreeRewriter.rewriteWith(new DummyRewriter(), node.getQuery(), null);
//      System.out.println(node2.accept(new NodeFormatter(), null));
//      tableManager.getTables().get(Name.system("Orders")).getTable().addColumns()
//
//
//      String CustomerOrderStats = "SELECT customerid, count(*) as num_orders\n"
//          + "                      FROM " + tableManager.getTables().get(Name.system("Orders")).getTable() + "\n"
//          + "                      GROUP BY customerid";
//
//
      TableEnvironmentImpl envImpl = ((TableEnvironmentImpl)env);
      List<Operation> operations = envImpl.getParser().parse(query);
      System.out.println(operations);

      Table table = env.sqlQuery(query);
      SqrlEntity queryEntity = new SqrlEntity(table);

      tableManager.getTables().put(node.getNamePath(), queryEntity);

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


      SqrlEntity contextEntity = tableManager.getTables().get(context.getCurrentContext());
      System.out.println(contextEntity);
//      Name name = Name.of(tableManager.getTables().get(Name.system(node.getName().toString())).getTable().toString(), AS_IS);
      NamePath postfix = node.getNamePath().popFirst();

      return new ai.dataeng.sqml.tree.Table(context.getCurrentContext().resolve(postfix));
    }
  }


  public class TableNameRewriter extends NodeRewriter {

    @Override
    public Node rewriteTable(ai.dataeng.sqml.tree.Table node, Object context,
        NodeTreeRewriter treeRewriter) {
      SqrlEntity entity = tableManager.getTables().get(node.getNamePath());
      Preconditions.checkNotNull(entity, node.getNamePath());
      Table table = tableManager.getTables().get(node.getNamePath()).getTable();

      return new ai.dataeng.sqml.tree.Table(NamePath.of(Name.of(table.toString(), AS_IS)));
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