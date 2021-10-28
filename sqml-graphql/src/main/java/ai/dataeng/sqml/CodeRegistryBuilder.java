package ai.dataeng.sqml;

import static graphql.schema.FieldCoordinates.coordinates;

import ai.dataeng.sqml.ViewQueryRewriter.ViewTable;
import ai.dataeng.sqml.schema2.Field;
import graphql.language.Selection;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLCodeRegistry;
import java.util.List;
import java.util.Map;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CodeRegistryBuilder {
  GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();

  public void buildQuery(String parentType, Field field, ViewTable viewTable) {
    codeRegistry.dataFetcher(coordinates(parentType, field.getName().getDisplay()),
        new DataFetcher<List<Map<String, Object>>>() {
          @Override
          public List<Map<String, Object>> get(DataFetchingEnvironment environment) throws Exception {
            GraphqlSqmlContext context = environment.getContext();

            System.out.println(viewTable);
            //Todo: push logic into plan node so predicates can be pushed into it

            //If node has context fields
//
//            Set<String> columns = node.getRelationDefinition().getFields()
//                .stream().map(e->e.getName().get()).collect(
//                Collectors.toSet());
//
//            List<SelectItem> items = environment.getField().getSelectionSet().getSelections()
//                .stream()
//                .filter(s->columns.contains(((graphql.language.Field)s).getName()))
//                .map(s->new SingleColumn(new Identifier(((graphql.language.Field)s).getName())))
//                .collect(Collectors.toList());
//
//            Query query = new Query(
//                new QuerySpecification(
//                    Optional.<NodeLocation>empty(),
//                    new Select(false, List.of(new AllColumns())),
//                    new Table(
//                        QualifiedName.of(node.getRelationDefinition().getRelationName().getParts().get(0))), //todo: Fix table name
//                    constructContextClause(node, environment.getSource()),
//                    Optional.<GroupBy>empty(),
//                    Optional.<Expression>empty(),
//                    Optional.<OrderBy>empty(),
//                    Optional.<String>empty()
//                ),
//                Optional.empty(),
//                Optional.empty()
//            );
//
//
//
//            NodeFormatter nodeFormatter = new NodeFormatter();
//            String queryStr = query.accept(nodeFormatter, null);
////            String query = (String)node.accept(h2QueryRewriter,
////                new GraphqlContext());
//            log.info(queryStr);
//            ResultSet rs = context.getConnection().createStatement()
//                .executeQuery(queryStr);
//            return toResult(rs);
            return List.of();
          }
      });
  }
//
//  private Optional<Expression> constructContextClause(PhysicalPlanNode node, Map<String, Object> source) {
//    if (node.getRelationDefinition().getContextKey().isEmpty()) {
//      return Optional.empty();
//    }
//    List<ComparisonExpression> comparisonExpressions = new ArrayList<>();
//    for (Field field : node.getRelationDefinition().getContextKey()) {
//      ComparisonExpression comparisonExpression = new ComparisonExpression(Operator.EQUAL,
//          new Identifier(field.getName().getDisplay()),
//          new LongLiteral(source.get(field.getName().getDisplay()).toString()));
//      comparisonExpressions.add(comparisonExpression);
//    }
//
//    return Optional.of(andComparisons(comparisonExpressions));
//  }
//
//  private Expression andComparisons(List<ComparisonExpression> comparisonExpressions) {
//    if (comparisonExpressions.size() == 1) {
//      return comparisonExpressions.get(0);
//    }
//
//    return new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, comparisonExpressions.get(0),
//        andComparisons(comparisonExpressions.subList(1, comparisonExpressions.size())));
//  }
//
//  @SneakyThrows
//  private List<Map<String, Object>> toResult(ResultSet rs) {
//    List<Map<String, Object>> list = new ArrayList<>();
//    while(rs.next()) {
//      ResultSetMetaData meta = rs.getMetaData();
//      int count = meta.getColumnCount();
//      ImmutableMap.Builder map = ImmutableMap.builderWithExpectedSize(count);
//      for (int i = 0; i < count; i++) {
//        map.put(meta.getColumnName(i+1), rs.getObject(i+1));
//      }
//      list.add(map.build());
//    }
//
//    return list;
//  }

  public GraphQLCodeRegistry build() {
      return codeRegistry.build();
    }

  @Value
  class GraphqlContext {
    List<Selection> selections;
  }
}
