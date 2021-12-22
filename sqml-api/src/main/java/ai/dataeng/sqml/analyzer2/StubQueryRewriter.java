package ai.dataeng.sqml.analyzer2;

import ai.dataeng.sqml.tree.name.Name;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

public class StubQueryRewriter {
//
//  private String rewriteQuery(QueryAssignment node, Analyzer2.Scope context, boolean useViewDefinition) {
//    Node n = node.getQuery();
//
//    HasContextTable ctx = new HasContextTable();
//    n.accept(ctx, null);
//    if (ctx.isHasContext()) {
//      SqrlEntity ent = tableManager.getTable(node.getNamePath().getPrefix().get());
//
//      if (!hasAggFunction(node.getQuery(), node.getNamePath(), ent)) {
//        n = NodeTreeRewriter.rewriteWith(new AddContextKeysToSelect(),
//            n,
//            new RewriterContext(node.getNamePath().getPrefix().get(), ent));
//      } else {
//        n = NodeTreeRewriter.rewriteWith(new AddGroupsToContextQuery(),
//            n,
//            new RewriterContext(node.getNamePath().getPrefix().get(), ent));
//      }
//      n = NodeTreeRewriter.rewriteWith(new DecontextualizeTableNames(),
//          n,
//          new RewriterContext(node.getNamePath().getPrefix().get(), ent));
//      System.out.println(n.accept(new NodeFormatter(), null));
//    }
//    n = NodeTreeRewriter.rewriteWith(new TableNameRewriter(useViewDefinition), n, null);
//
//    String query = n.accept(new NodeFormatter(), null);
//
//    return query;
//  }
//
//  private boolean hasAggFunction(Query query, NamePath namePath,
//      SqrlEntity ent) {
//    //Decontextualize current query and run it through parser
//    Node names = NodeTreeRewriter.rewriteWith(new DecontextualizeTableNames(),
//        query,
//        new RewriterContext(namePath.getPrefix().get(), ent));
//    names = NodeTreeRewriter.rewriteWith(new TableNameRewriter(false), names, null);
//
//    String queryStr = names.accept(new NodeFormatter(), null);
//    System.out.println(queryStr);
//    PlannerQueryOperation plannerQueryOperation = (PlannerQueryOperation)((TableEnvironmentImpl) env).getParser().parse(queryStr).get(0);
//
//    //The top node may not be an aggregate
//
//    RelNode node = plannerQueryOperation.getCalciteTree();
//    //TODO: We can't do this because we won't know if the calite tree is constrained
//    // to the current query.
////      while (node instanceof Project) {
////        node = ((Project)node).getInput();
////      }
//    if (node instanceof LogicalAggregate) {
//      LogicalAggregate logicalAggregate = (LogicalAggregate) node;
//      return !logicalAggregate.getAggCallList().isEmpty();
//    }
//
//    return false;
//  }

  public static List<Name> extractPrimaryKey(TableEnvironment env, String query) {
    List<Name> keys = new ArrayList<>();

    //TODO: instead of reparsing, get it from temp table
    PlannerQueryOperation plannerQueryOperation = (PlannerQueryOperation)((TableEnvironmentImpl) env).getParser().parse(query).get(0);

    //The top node may not be an aggregate
    RelNode node = plannerQueryOperation.getCalciteTree();
//      while (node instanceof Project) {
//        node = ((Project)node).getInput();
//      }

    if (node instanceof LogicalAggregate) {
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

      if (keys.isEmpty()) {
        for (RelDataTypeField field : logicalAggregate.getRowType().getFieldList()) {
          keys.add(Name.system(field.getName()));
        }
      }

    } else {
      //No grouping keys, use all output fields

      RelRecordType inputFields = (RelRecordType)plannerQueryOperation.getCalciteTree().getRowType();
      for (RelDataTypeField field : inputFields.getFieldList()) {
        //TODO: The table generated shouldn't use these as their primary keys but nested queries should
        // consider it to be its context keys.

//         keys.add(Name.system(field.getName()));
      }

    }
    return keys;
  }
//
//
//  @Value
//  public class RewriterContext {
//    NamePath currentContext;
//    SqrlTable currentContextEntity;
//  }
//
//  public class HasContextTable extends DefaultTraversalVisitor {
//    boolean hasContext = false;
//    @Override
//    public Object visitTable(ai.dataeng.sqml.tree.Table node, Object context) {
//      if (node.getNamePath().get(0).getDisplay().equalsIgnoreCase("@")) {
//        hasContext = true;
//      }
//
//      return null;
//    }
//
//    public boolean isHasContext() {
//      return hasContext;
//    }
//  }
//
//
//  public class AddContextKeysToSelect extends NodeRewriter<RewriterContext> {
//
//    @Override
//    public Select rewriteSelect(Select node, RewriterContext context,
//        NodeTreeRewriter<RewriterContext> treeRewriter) {
//      List<SelectItem> selectItems = new ArrayList<>();
//      selectItems.addAll(getContextKeys(context).stream().map(e->new SingleColumn(e)).collect(
//          Collectors.toList()));
//      selectItems.addAll(node.getSelectItems());
//
//      return new Select(node.isDistinct(), selectItems);
//    }
//
//    public List<Identifier> getContextKeys(RewriterContext context) {
//      return context.getCurrentContextEntity().getContextKey()
//          .stream()
//          .map(e->new Identifier(e.getDisplay()))
//          .collect(Collectors.toList());
//    }
//  }
//
//  public class AddGroupsToContextQuery extends NodeRewriter<RewriterContext> {
//
//    @Override
//    public Select rewriteSelect(Select node, RewriterContext context,
//        NodeTreeRewriter<RewriterContext> treeRewriter) {
//      List<SelectItem> selectItems = new ArrayList<>();
//      selectItems.addAll(getContextKeys(context).stream().map(e->new SingleColumn(e)).collect(
//          Collectors.toList()));
//      selectItems.addAll(node.getSelectItems());
//
//      return new Select(node.isDistinct(), selectItems);
//    }
//
//    public List<Identifier> getContextKeys(RewriterContext context) {
//      return context.getCurrentContextEntity().getContextKey()
//          .stream()
//          .map(e->new Identifier(e.getDisplay()))
//          .collect(Collectors.toList());
//    }
//    @Override
//    public Node rewriteGroupBy(GroupBy node, RewriterContext context,
//        NodeTreeRewriter<RewriterContext> treeRewriter) {
//      return createGroupingExpressions(context, node.getGroupingElement().getExpressions());
//    }
//
//    public GroupBy createGroupingExpressions(RewriterContext context, List<Expression> additional) {
//      List<Expression> grouping = new ArrayList();
//      grouping.addAll(getContextKeys(context));
//      grouping.addAll(additional);
//      GroupingElement element = new SimpleGroupBy(grouping);
//      return new GroupBy(element);
//    }
//
//    @Override
//    public Node rewriteQuerySpecification(QuerySpecification node, RewriterContext context,
//        NodeTreeRewriter<RewriterContext> treeRewriter) {
//      Select select = rewriteSelect(node.getSelect(), context, treeRewriter);
//      Relation from = NodeTreeRewriter.rewriteWith(this, node.getFrom(),
//          context);
////      Optional<Expression> where = node.getWhere()
////          .map(value -> rewrite(value, context));
//      Node groupBy = node.getGroupBy()
//          .map(value -> rewriteGroupBy(value, context, treeRewriter))
//          .orElseGet(() -> createGroupingExpressions(context, List.of()));
//
//
//      return new QuerySpecification(
//          node.getLocation(),
//          select,
//          from,
//          node.getWhere(),
//          Optional.of((GroupBy) groupBy),
//          node.getHaving(),
//          node.getOrderBy(),
//          node.getLimit()
//      );
//    }
//  }
//
//  public class DecontextualizeTableNames extends NodeRewriter<RewriterContext> {
//
//    @Override
//    public Node rewriteTable(ai.dataeng.sqml.tree.Table node, RewriterContext context,
//        NodeTreeRewriter treeRewriter) {
//      SqrlTable contextEntity = tableManager.getTable(context.getCurrentContext());
//      System.out.println(contextEntity);
////      Name name = Name.of(tableManager.getTable(Name.system(node.getName().toString())).getTable().toString(), AS_IS);
//      NamePath postfix = node.getNamePath().popFirst();
//
//      return new ai.dataeng.sqml.tree.Table(context.getCurrentContext().resolve(postfix));
//    }
//  }
//
//
//  @AllArgsConstructor
//  public class TableNameRewriter extends NodeRewriter {
//    boolean useViewDefinition;
//    @Override
//    public Node rewriteTable(ai.dataeng.sqml.tree.Table node, Object context,
//        NodeTreeRewriter treeRewriter) {
//      MaterializeTable tbl = tableManager.getMatTable(node.getNamePath());
//      Preconditions.checkNotNull(tbl, node.getNamePath());
//      String name;
//      if (useViewDefinition) {
//        name = tableManager.getCurrentName(tbl.getEntity());
//      } else {
//        name = tbl.getEntity().getTable().toString();
//      }
//
//      return new ai.dataeng.sqml.tree.Table(NamePath.of(Name.of(name, AS_IS)));
//    }
//  }

}
