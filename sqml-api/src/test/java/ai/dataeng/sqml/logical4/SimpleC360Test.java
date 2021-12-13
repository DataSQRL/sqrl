package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.logical4.AggregateOperator.Aggregation;
import ai.dataeng.sqml.logical4.LogicalPlan.DocumentNode;
import ai.dataeng.sqml.logical4.LogicalPlan.Node;
import ai.dataeng.sqml.logical4.LogicalPlan.RowNode;
import ai.dataeng.sqml.relation.CallExpression;
import ai.dataeng.sqml.relation.ConstantExpression;
import ai.dataeng.sqml.relation.RowExpression;
import ai.dataeng.sqml.relation.VariableReferenceExpression;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.BooleanType;
import ai.dataeng.sqml.schema2.basic.DateTimeType;
import ai.dataeng.sqml.schema2.basic.IntegerType;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

public class SimpleC360Test {
//
//  @Test
//  public void testSimpleC360() {
//    /**
//     * CustomerOrderStats := SELECT customerid, count(*) as num_orders FROM Orders
//     *                       WHERE time % 2 = 0
//     *                       GROUP BY customerid;
//     */
//    Node plan =
//      agg(
//          Map.of(variable("num_orders", IntegerType.INSTANCE), aggregation(call("count", IntegerType.INSTANCE, List.of()))),
//          List.of(variable("customerid", IntegerType.INSTANCE)),
//          project(
//            Map.of(variable("customerid", IntegerType.INSTANCE), variable("customerid", IntegerType.INSTANCE)),
//            Locality.LOCAL,
//            filter(
//              call("=", BooleanType.INSTANCE, List.of(call("%", IntegerType.INSTANCE, List.of(variable("time",
//                  DateTimeType.INSTANCE))), constant("0", IntegerType.INSTANCE))),
//              shred(
//                  "orders",
//                  List.of(variable("customerid", IntegerType.INSTANCE), variable("time", DateTimeType.INSTANCE)),
//                  source(
//                      List.of(variable("customerid", IntegerType.INSTANCE), variable("time", DateTimeType.INSTANCE))
//                  )
//              )
//            )
//          )
//      );
//  }
//
//  private Aggregation aggregation(CallExpression expression) {
//    return new Aggregation(expression);
//  }
//
//  private RowNode project(Map<VariableReferenceExpression, RowExpression> assignments, Locality locality,
//      RowNode source) {
//    return new ProjectOperator(source, assignments, locality);
//  }
//
//  private ConstantExpression constant(Object constant, Type type) {
//    return new ConstantExpression(constant, type);
//  }
//
//  private VariableReferenceExpression variable(String name, Type type) {
//    return new VariableReferenceExpression(name, type);
//  }
//
//  private CallExpression call(String name, Type type,
//      List<RowExpression> arguments) {
//    return new CallExpression(name, null, type, arguments);
//  }
//
//  private DocumentNode source(List<VariableReferenceExpression> outputVariables) {
//    return new DocumentSource(null, null, null, outputVariables);
//  }
//
//  private RowNode shred(String name, List<VariableReferenceExpression> output, DocumentNode source) {
//    return new ShreddingOperator(null, NamePath.of(Name.system(name)), null, null, output);
//  }
//
//  private RowNode filter(RowExpression predicate, RowNode source) {
//    return new FilterOperator(source, predicate);
//  }
//
//  private Node agg(Map<VariableReferenceExpression, Aggregation> aggs,
//      List<VariableReferenceExpression> groupingKeys,
//      RowNode source) {
//    return new AggregateOperator(source, aggs, groupingKeys);
//  }
}