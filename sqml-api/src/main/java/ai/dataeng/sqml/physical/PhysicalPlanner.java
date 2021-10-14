package ai.dataeng.sqml.physical;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.SqlQueryRewriter;
import ai.dataeng.sqml.env.SqmlEnv;
import ai.dataeng.sqml.logical.DistinctRelationDefinition;
import ai.dataeng.sqml.logical.ExtendedChildQueryRelationDefinition;
import ai.dataeng.sqml.logical.ExtendedChildRelationDefinition;
import ai.dataeng.sqml.logical.ExtendedFieldRelationDefinition;
import ai.dataeng.sqml.logical.ImportRelationDefinition;
import ai.dataeng.sqml.logical.LogicalPlan;
import ai.dataeng.sqml.logical.QueryRelationDefinition;
import ai.dataeng.sqml.logical.RelationDefinition;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeFormatter;
import ai.dataeng.sqml.tree.NodeLocation;
import ai.dataeng.sqml.tree.OrderBy;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.Table;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PhysicalPlanner extends SqmlTypeVisitor<PhysicalPlanNode, Object> {
  private final SqmlEnv env;
  private final Analysis analysis;
  private final Metadata metadata;
  private final PhysicalPlan physicalPlan;

  AtomicInteger viewIncrementer = new AtomicInteger();

  public PhysicalPlanner(SqmlEnv env, Analysis analysis, Metadata metadata) {
    this.env = env;
    this.analysis = analysis;
    this.metadata = metadata;
    physicalPlan = new PhysicalPlan(analysis.getLogicalPlan());
  }

  public PhysicalPlanNode visitPlan(LogicalPlan logicalPlan, Object context) {
//    for (RelationDefinition entry : logicalPlan.getTableDefinitions()) {
//      entry.accept(this, context);
//    }
    return null;
  }

  @Override
  public PhysicalPlanNode visitImportTableDefinition(ImportRelationDefinition rel, Object context) {
//    Optional<PhysicalPlanNode> seen;
//    if ((seen = setAndCheckSeen(rel)).isPresent()) {
//      return seen.get();
//    }
//
//    List<Column> columns = new ArrayList<>();
//    for (Field field : rel.getFields()) {
//      columns.add(new Column(field, field.getName().get()));
//    }
//
//    //TODO: Stats should return these fields?
//    columns.add(new Column(Field.newDataField("__timestamp", new StringType()),
//        "__timestamp"));
//    columns.add(new Column(Field.newDataField("_uuid", new UuidType()),
//        "_uuid"));

    return null;//createPhysicalPlanNode(rel.getSourceTable().getName().getDisplay(), columns, rel);
  }

  private PhysicalPlanNode createPhysicalPlanNode(String tableName, List<Column> columns,
      RelationDefinition relation) {
    PhysicalPlanNode physicalPlanNode = new TablePlanNode(relation, tableName, columns);
    physicalPlan.mapper.put(relation, physicalPlanNode);
    return physicalPlanNode;
  }

  @Override
  @SneakyThrows
  public PhysicalPlanNode visitExtendedRelation(ExtendedFieldRelationDefinition rel, Object context) {
    Optional<PhysicalPlanNode> seen;
    if ((seen = setAndCheckSeen(rel)).isPresent()) {
      return seen.get();
    }

    RelationDefinition parentTable = rel.getParent();
    PhysicalPlanNode parentPlanNode = parentTable.accept(this, context);

    //Select items are all extended fields + the extended field
    List<Column> selectColumns = new ArrayList<>(parentPlanNode.getColumns());
    List<SelectItem> items = toSelectItems(selectColumns);
    H2NestedExpressionRewriter h2QueryRewriter = new H2NestedExpressionRewriter(metadata);
    SelectItem item = (SelectItem)rel.getNode().accept(h2QueryRewriter, new RewriteScope(parentTable.getFields(), //todo primary keys
        rel.getName()));
    if (item == null) {

      throw new RuntimeException("Expression could not be translated, TBD features");
    }

    Query query = new Query(
        new QuerySpecification(
            Optional.<NodeLocation>empty(),
            new Select(false, items),
            new Table(rel.getRelationName()),
            Optional.<Expression>empty(),
            Optional.<GroupBy>empty(),
            Optional.<Expression>empty(),
            Optional.<OrderBy>empty(),
            Optional.<String>empty()
        ),
        Optional.empty(),
        Optional.empty()
    );

    String entityName = rel.getRelationName().getParts().get(0);
    String tableName = String.format("%s_%d", entityName, viewIncrementer.incrementAndGet());

    TranslationMapper translationMapper = new TranslationMapper();
    Node rewritten = query.accept(translationMapper, null);

    NodeFormatter nodeFormatter = new NodeFormatter();

    String sqlQuery = String.format("create or replace view %s as %s ", tableName,
        nodeFormatter.process(rewritten));

    System.out.println(sqlQuery);
//    env.getConnectionProvider().getOrEstablishConnection()
//        .createStatement().execute(sqlQuery);

    return createPhysicalPlanNode(tableName, List.of(), rel);
  }

  private List<SelectItem> toSelectItems(List<Column> columns) {
    List<SelectItem> items = new ArrayList<>();
    for (Column column : columns) {
      if (column.getField().getName() == null) {
        log.info("Missing column");
        continue;
      }
//      items.add(new SingleColumn(new Identifier(column.getField().getName().get())));
    }

    return items;
  }

  @Override
  @SneakyThrows
  public PhysicalPlanNode visitDistinctRelation(DistinctRelationDefinition rel,
      Object context) {
//    Optional<PhysicalPlanNode> seen;
//    if ((seen = setAndCheckSeen(rel)).isPresent()) {
//      return seen.get();
//    }
//
//    PhysicalPlanNode parentPlanNode = rel.getParent().accept(this, context);
//
//    List<Column> selectColumns = new ArrayList<>(parentPlanNode.getColumns());
//    List<SelectItem> items = toSelectItems(selectColumns);
//
//    Query queryNode = new Query(
//        new QuerySpecification(
//            Optional.<NodeLocation>empty(),
//            new Select(new DistinctOn(rel.getExpression().getFields()), items),
//            new Table(QualifiedName.of(((TablePlanNode)parentPlanNode).getTableName())), //todo move to translation
//            Optional.<Expression>empty(),
//            Optional.<GroupBy>empty(),
//            Optional.<Expression>empty(),
//            Optional.of(new OrderBy(List.of(
//                new SortItem(new Identifier("__timestamp"), Ordering.DESCENDING)))),
//            Optional.<String>empty()
//        ),
//        Optional.empty(),
//        Optional.empty()
//    );
//
//    TranslationMapper translationMapper = new TranslationMapper();
//    NodeFormatter nodeFormatter = new NodeFormatter();
//    Node rewritten = queryNode.accept(translationMapper, null);
//
//    String entityName = rel.getRelationName().getParts().get(0);
//    String tableName = String.format("%s_%d", entityName, viewIncrementer.incrementAndGet());
//
//    String query = String.format("create or replace view %s as %s ", tableName,
//      nodeFormatter.process(rewritten));
//
//    System.out.println(query);
////          + "select distinct ON(productid) productid, name, description, category, __timestamp, _uuid "
////        + " from product order by __timestamp desc;";
////    entityMapping.put("product", "product_0");
//    env.getConnectionProvider().getOrEstablishConnection()
//          .createStatement().execute(query);
//    return createPhysicalPlanNode(tableName,  List.of(), rel);
    return null;
  }

  @Override
  public PhysicalPlanNode visitRelationDefinition(RelationDefinition relation, Object context) {
    return createPhysicalPlanNode("product", List.of(), relation);
  }

  @Override
  public PhysicalPlanNode visitQueryRelationDefinition(QueryRelationDefinition relation,
      Object context) {
    SqlQueryRewriter queryRewriter = new SqlQueryRewriter(relation, null);
    relation.getQuery().accept(queryRewriter, null);


    return createPhysicalPlanNode("product", List.of(), relation);
  }

  @Override
  public PhysicalPlanNode visitExtendedChild(ExtendedChildRelationDefinition relation,
      Object context) {
    return createPhysicalPlanNode("product", List.of(), relation);
  }

  @Override
  public PhysicalPlanNode visitExtendedChildQueryRelation(
      ExtendedChildQueryRelationDefinition rel, Object context) {
    rel.getParent().accept(this, context);
     return createPhysicalPlanNode("product",  List.of(), rel);
  }

  public PhysicalPlan getPhysicalPlan() {
    return physicalPlan;
  }

  private Optional<PhysicalPlanNode> setAndCheckSeen(RelationDefinition table) {
    return Optional.ofNullable(physicalPlan.mapper.get(table));
  }
}