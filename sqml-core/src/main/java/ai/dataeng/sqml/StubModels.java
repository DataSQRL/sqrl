package ai.dataeng.sqml;

import ai.dataeng.sqml.common.ColumnHandle;
import ai.dataeng.sqml.common.type.DoubleType;
import ai.dataeng.sqml.common.type.IntegerType;
import ai.dataeng.sqml.common.type.Type;
import ai.dataeng.sqml.common.type.UnknownType;
import ai.dataeng.sqml.common.type.VarcharType;
import ai.dataeng.sqml.metadata.ColumnMetadata;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.metadata.TableMetadata;
import ai.dataeng.sqml.plan.AggregationNode;
import ai.dataeng.sqml.plan.AggregationNode.Aggregation;
import ai.dataeng.sqml.plan.AggregationNode.GroupingSetDescriptor;
import ai.dataeng.sqml.plan.AggregationNode.Step;
import ai.dataeng.sqml.plan.Assignments;
import ai.dataeng.sqml.plan.JoinNode;
import ai.dataeng.sqml.plan.JoinNode.EquiJoinClause;
import ai.dataeng.sqml.plan.PlanNode;
import ai.dataeng.sqml.plan.ProjectNode;
import ai.dataeng.sqml.plan.TableScanNode;
import ai.dataeng.sqml.planner.PlanNodeIdAllocator;
import ai.dataeng.sqml.relation.CallExpression;
import ai.dataeng.sqml.relation.ConstantExpression;
import ai.dataeng.sqml.relation.RowExpression;
import ai.dataeng.sqml.relation.TableHandle;
import ai.dataeng.sqml.relation.VariableReferenceExpression;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 1. Create what this output should look like
 *
 * create materialized view Entries_view as
 * select _parent_id, _id, quantity * unit_price - COALESCE(discount, 0.0) as total, quantity, unit_price, discount
 * from (
 *   select orders.json->'_id' as _parent_id,
 *   (entries->'_id')::text as _id,
 *   (entries->'quantity_number')::integer as quantity,
 *   (entries->'unit_price_number')::float as unit_price,
 *   (entries->'discount_number')::float as discount
 *   from orders, jsonb_array_elements(json->'entries_object') entries
 * ) e;
 *
 * create materialized view Orders_view as
 * select
 *     o._id,
 *     o.customerid,
 *     e1.total,
 *     e2.total_savings
 * from (select (json->'_id')::text as _id, (json->'customerid_number')::integer as customerid from orders) o
 *          left outer join (select _parent_id, sum(e.total) as total
 *                           from Entries_view e
 *                           group by e._parent_id) e1 on o._id::text = e1._parent_id::text
 *          left outer join (select _parent_id, sum(COALESCE(e.discount, 0.0)) as total_savings
 *                           from Entries_view e
 *                           group by e._parent_id) e2 on o._id::text = e2._parent_id::text;
 */

public class StubModels {

  public static PlanNodeIdAllocator allocator = new PlanNodeIdAllocator();
  public ProjectNode entries_rel;
  public ProjectNode order_view;

  public StubModels() {
  }
  public void generate() {
    /**
     *
     create materialized view Entries_view as
     //project(_parent_id, _id, expression(quantity * unit_price - expression(COALESCE(discount, 0.0))) as total, quantity, unit_price, discount
     //project orders.json->'_id' as _parent_id,
     //       (entries->'_id')::text as _id,
     //       (entries->'quantity_number')::integer as quantity,
     //       (entries->'unit_price_number')::float as unit_price,
     //       (entries->'discount_number')::float as discount
     //join( orders, function('jsonb_array_elements', json->'entries_object'))

     select _parent_id, _id, quantity * unit_price - COALESCE(discount, 0.0) as total, quantity, unit_price, discount
       from (
       select orders.json->'_id' as _parent_id,
       (entries->'_id')::text as _id,
       (entries->'quantity_number')::integer as quantity,
       (entries->'unit_price_number')::float as unit_price,
       (entries->'discount_number')::float as discount
       from orders, jsonb_array_elements(json->'entries_object') entries
     ) e;
     */
    //select entries->'_id' from (select jsonb_array_elements(json->'entries_OBJECT') entries from orders) p limit 1
    TableScanNode orders = scan("orders", Map.of(
        variable("json", UnknownType.UNKNOWN), new ColumnHandle() {}
    ));
    ProjectNode p = project(orders, assignments(Map.of(
        variable("orders.json", UnknownType.UNKNOWN), variable("orders.json", UnknownType.UNKNOWN),
        variable("entries", UnknownType.UNKNOWN),
          call("jsonb_array_elements", List.of(variable("json.entries_OBJECT", UnknownType.UNKNOWN)))
    )));
    ProjectNode c = project(p, assignments(Map.of(
      variable("_parent_id", VarcharType.VARCHAR),
        call("cast", List.of(variable("orders.json._id", UnknownType.UNKNOWN))),
      variable("_id", VarcharType.VARCHAR),
        call("cast", List.of(variable("entries._id", UnknownType.UNKNOWN))),
      variable("quantity", IntegerType.INTEGER),
        call("cast", List.of(variable("entries.quantity_NUMBER", UnknownType.UNKNOWN))),
      variable("unit_price", DoubleType.DOUBLE),
        call("cast", List.of(variable("entries.unit_price_NUMBER", UnknownType.UNKNOWN))),
      variable("discount", DoubleType.DOUBLE),
        call("cast", List.of(variable("entries.discount_NUMBER", UnknownType.UNKNOWN)))
    )));

    ProjectNode q = project(
        c,
        assignments(Map.of(
            variable("_parent_id", VarcharType.VARCHAR), variable("_parent_id", VarcharType.VARCHAR),
            variable("_id", VarcharType.VARCHAR), variable("_id", VarcharType.VARCHAR),
            variable("total", DoubleType.DOUBLE),
              call("multiply", List.of(variable("quantity", IntegerType.INTEGER),
                  call("subtract", List.of(variable("unit_price", DoubleType.DOUBLE),
                      call("COALESCE", List.of(variable("discount", DoubleType.DOUBLE),
                          doubleLiteral(0.0))))))),//quantity * unit_price - COALESCE(discount, 0.0)
            variable("quantity", IntegerType.INTEGER), variable("quantity", IntegerType.INTEGER),
            variable("unit_price", DoubleType.DOUBLE), variable("unit_price", DoubleType.DOUBLE),
            variable("discount", DoubleType.DOUBLE), variable("discount", DoubleType.DOUBLE)
        ))
    );
    this.entries_rel = q;
    System.out.println(q);
    /**
     *
create materialized view Orders_view as
select
    o._id,
    o.customerid,
    e1.total,
    e2.total_savings
from (select (json->'_id')::text as _id, (json->'customerid_number')::integer as customerid from orders) o
         left outer join (select _parent_id, sum(e.total) as total, sum(COALESCE(e.discount, 0.0)) as total_savings
                          from Entries_view e
                          group by e._parent_id) e1 on o._id::text = e1._parent_id::text
     */
    AggregationNode agg = aggregate(
        scan("Entries_view", Map.of(
            variable("_parent_id", UnknownType.UNKNOWN), new ColumnHandle() {},
            variable("total", UnknownType.UNKNOWN), new ColumnHandle() {},
            variable("discount", UnknownType.UNKNOWN), new ColumnHandle() {}
        )),
        Map.of(
          variable("total", UnknownType.UNKNOWN),
            aggregation(call("sum", List.of(variable("total", UnknownType.UNKNOWN)))),
          variable("total_savings", UnknownType.UNKNOWN),
            aggregation(call("sum", List.of(call("coalesce", List.of(variable("discount", UnknownType.UNKNOWN), doubleLiteral(0.0))))))
        ),
        grouping(List.of(variable("_parent_id", UnknownType.UNKNOWN)))
    );

    ProjectNode ordersProject = project(
        scan("orders", Map.of(variable("json", UnknownType.UNKNOWN), new ColumnHandle() {})),
        assignments(Map.of(
            variable("_id", VarcharType.VARCHAR), call("cast", List.of(variable("_id", UnknownType.UNKNOWN))),
            variable("customerid", IntegerType.INTEGER), call("cast", List.of(variable("customerid_NUMBER", UnknownType.UNKNOWN)))
        ))
    );

    JoinNode join = join(JoinNode.Type.INNER, agg, ordersProject,
        List.of(criteria(variable("_id", VarcharType.VARCHAR), variable("_parent_id", VarcharType.VARCHAR))),
        List.of(variable("_id", VarcharType.VARCHAR),
            variable("customerid", UnknownType.UNKNOWN),
            variable("total", UnknownType.UNKNOWN),
            variable("total_savings", UnknownType.UNKNOWN))
    );

    ProjectNode projectNode = project(join,
        assignments(Map.of(
            variable("_id", VarcharType.VARCHAR),
            variable("customerid", VarcharType.VARCHAR),
            variable("total", VarcharType.VARCHAR),
            variable("total_savings", VarcharType.VARCHAR)
        )));

    System.out.println(projectNode);
    this.order_view = projectNode;
  }

  private static EquiJoinClause criteria(VariableReferenceExpression left, VariableReferenceExpression right) {
    return new EquiJoinClause(
        left, right
    );
  }

  private static Aggregation aggregation(CallExpression callExpression) {
    return new Aggregation(
        callExpression,
        Optional.empty(),
        Optional.empty(),
        false,
        Optional.empty()
    );
  }

  private static GroupingSetDescriptor grouping(List<VariableReferenceExpression> keys) {
    return new GroupingSetDescriptor(
        keys,
        keys.size(),
        Set.of()
    );
  }

  private static AggregationNode aggregate(PlanNode source,
      Map<VariableReferenceExpression, Aggregation> aggregations,
      GroupingSetDescriptor grouping) {
    return new AggregationNode(
        allocator.getNextId(),
        source,
        aggregations,
        grouping,
        List.of(),
        Step.SINGLE,
        Optional.empty(),
        Optional.empty()
    );
  }

  private static ConstantExpression doubleLiteral(double v) {
    return new ConstantExpression(v, DoubleType.DOUBLE);
  }

  private static TableScanNode scan(String tableName, Map<VariableReferenceExpression, ColumnHandle> assignments) {
    return new TableScanNode(
        allocator.getNextId(),
        new TableHandle(tableName),
        new ArrayList<>(assignments.keySet()),
        assignments
    );
  }

  private static CallExpression call(String name, List<RowExpression> arguments) {
    return new CallExpression(
        name,
        () -> null,
        UnknownType.UNKNOWN,
        arguments
    );
  }

  private static VariableReferenceExpression variable(String name, Type type) {
    return new VariableReferenceExpression(name, type);
  }

  private static Assignments assignments(Map<VariableReferenceExpression, RowExpression> assignments) {
    return new Assignments(assignments);
  }

  private static JoinNode join(JoinNode.Type type, PlanNode left, PlanNode right, List<EquiJoinClause> criteria, List<VariableReferenceExpression> outputVariables) {
    return new JoinNode(
        allocator.getNextId(),
        type,
        left,
        right,
        criteria,
        outputVariables,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Map.of()
      );
  }

  private static ProjectNode project(PlanNode source,
      Assignments assignments) {
    return new ProjectNode(
        allocator.getNextId(),
        source,
        assignments
    );
  }


  //////////

  //Todo: Notes: Look at the time-based interaction between the metadata and script analyzer
  public static ProjectNode createSelectAllStub(Metadata metadata) {
    PlanNodeIdAllocator allocator = new PlanNodeIdAllocator();
    TableHandle tableHandle = new TableHandle("orders");
    //Resolve columns from metadata for "orders"
    TableMetadata tableMetadata = metadata.getTableMetadata(null, tableHandle);
    List<VariableReferenceExpression> outputs = buildColumns(tableMetadata);
    TableScanNode orders = new TableScanNode(allocator.getNextId(),
        tableHandle,
        outputs,
        buildAssignments(outputs)
    );

    ProjectNode projectNode = new ProjectNode(allocator.getNextId(), orders, buildAssignment(outputs));

    return projectNode;
  }

  private static Assignments buildAssignment(List<VariableReferenceExpression> outputs) {
    Assignments assignments = new Assignments(buildRowAssignments(outputs));
    return assignments;
  }

  private static Map<VariableReferenceExpression, RowExpression> buildRowAssignments(
      List<VariableReferenceExpression> outputs) {
    Map<VariableReferenceExpression, RowExpression> rows = new HashMap<>();
    for (VariableReferenceExpression ref : outputs) {
      rows.put(ref, ref);
    }
    return rows;
  }

  private static Map<VariableReferenceExpression, ColumnHandle> buildAssignments(
      List<VariableReferenceExpression> columns) {
    Map<VariableReferenceExpression, ColumnHandle> rows = new HashMap<>();
    for (VariableReferenceExpression ref : columns) {
      rows.put(ref, new ColumnHandle() {});
    }
    return rows;
  }

  private static List<VariableReferenceExpression> buildColumns(TableMetadata tableMetadata) {
    List<VariableReferenceExpression> col = new ArrayList<>();
    for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
      col.add(new VariableReferenceExpression(columnMetadata.getName(), columnMetadata.getType()));
    }
    return col;
  }

}
