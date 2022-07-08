package ai.datasqrl.plan.local.analyzer;

import static org.mockito.Mockito.mock;

import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.local.JoinPathBuilder;
import ai.datasqrl.plan.local.SqlAliasIdentifier;
import ai.datasqrl.plan.local.SqlJoinDeclaration;
import ai.datasqrl.schema.Relationship;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.jupiter.api.Test;

/**
 * TODO today:
 * 1. Expand relationships to SQL Nodes to include primary keys (join declarations)
 * 2. Expand joins
 * 3.
 *
 */
class QueryGeneratorTest {


//
//  @Value
//  public static class Ctx {
//    String currentAlias;
//  }
//
//  //SELECT * FROM _ LEFT JOIN _.orders.entries ON _.customerid > 5;
//  //select * FROM Customer _ JOIN Orders o ON _.customerid = o.customerid LEFT JOIN entries e ON o._uuid = e._uuid;
//  public void expand(ResolvedNamePath part, Ctx ctx) {
//    JoinPathBuilder joinPathBuilder = new JoinPathBuilder();
//    joinPathBuilder.setCurrentAlias("_");
//
//    List<Field> path = part.getPath();
//    for (int i = 0; i < path.size(); i++) {
//      Field field = path.get(i);
//      if (field instanceof Relationship) {
//        if (i == path.size() - 1 && part.getAlias().isPresent()) {
//          joinPathBuilder.join((Relationship)field, part.getAlias().get());
//        } else {
//          joinPathBuilder.join((Relationship)field);
//        }
//      } else {
//
//      }
//    }
//
//    joinPathBuilder.getSqlNode();
//    joinPathBuilder.getTrailingCondition();
//    //FROM _ as self LEFT JOIN self.orders.entries e ON self.customerid > 5;
//    //Part is an alias:
//    // 1. Pass to next w/ alias (self)
//    //Part is a relationship & has alias context:
//    // 1. get join condition from rel:
//    // JOIN Orders o on o.customerid = _.customerid;
//    // 2. Replace context alias w/ current context alias
//    // JOIN Orders o on o.customerid = self.customerid;
//    // 3. Replace table aliases with new aliases:
//    // JOIN Orders o0 ON o0.customerid = self.customerid
//    //Move to next part: is relationship, repeat process
//    // JOIN entries e0 ON e0._uuid = o0._uuid
//    //Return token tree:
//    // (JOIN Orders o0 ON o0.customerid = self.customerid JOIN entries e0 ON e0._uuid = o0._uuid)
//    // if there are additional conditions, append them
//    //FROM Customer as self JOIN (Orders o0 JOIN entries e0 ON e0._uuid = o0._uuid) ON self.customerid > 5 AND o0.customerid = self.customerid;
//
//  }

  @Test
  public void testTransformers() {
    //1. Construct fields
    //2. Construct join declarations
    //3. Do it!

//    Table orders = new Table(0, NamePath.of("Orders"), Type.STREAM, new ShadowingContainer<>(), null, null);
//    Table entries = new Table(1, NamePath.of("Orders.entries"), Type.STREAM, new ShadowingContainer<>(), null, null);
    Relationship relationship = mock(Relationship.class);
    Relationship relationship2 = mock(Relationship.class);
//    orders.getFields().add(relationship);

    SqlNode condition = new SqlBasicCall(
        SqrlOperatorTable.EQUALS, new SqlNode[]{
          new SqlAliasIdentifier(List.of("_","_uuid"), SqlParserPos.ZERO),
          new SqlAliasIdentifier(List.of("e","_uuid"), SqlParserPos.ZERO),
    }, SqlParserPos.ZERO);

    SqlTableRef sqlTableRef = new SqlTableRef(SqlParserPos.ZERO, new SqlIdentifier("entries", SqlParserPos.ZERO), new SqlNodeList(SqlParserPos.ZERO));
    SqlBasicCall aliasedRef = new SqlBasicCall(
        SqrlOperatorTable.AS, new SqlNode[]{
            sqlTableRef,
            new SqlIdentifier("e", SqlParserPos.ZERO)
        },
      SqlParserPos.ZERO);

//2
    SqlNode condition2 = new SqlBasicCall(
        SqrlOperatorTable.EQUALS, new SqlNode[]{
        new SqlAliasIdentifier(List.of("_","customerid"), SqlParserPos.ZERO),
        new SqlAliasIdentifier(List.of("c","customerid"), SqlParserPos.ZERO),
    }, SqlParserPos.ZERO);

    SqlBasicCall aliasedRef2 = new SqlBasicCall(
        SqrlOperatorTable.AS, new SqlNode[]{
        new SqlTableRef(SqlParserPos.ZERO, new SqlIdentifier("customer", SqlParserPos.ZERO), new SqlNodeList(SqlParserPos.ZERO)),
        new SqlIdentifier("c", SqlParserPos.ZERO)
    }, SqlParserPos.ZERO);

    SqlJoinDeclaration declaration = new SqlJoinDeclaration(aliasedRef, condition);
    SqlJoinDeclaration declaration2 = new SqlJoinDeclaration(aliasedRef2, condition2);

    JoinPathBuilder builder = new JoinPathBuilder();
    builder.joinDeclarations.put(relationship, declaration);
    builder.joinDeclarations.put(relationship2, declaration2);

    builder.setCurrentAlias("m");
    builder.join(relationship);
    builder.join(relationship2, "e");


    SqlPrettyWriter sqlWriter = new SqlPrettyWriter();
    sqlWriter.startList("", "");
    builder.getSqlNode().unparse(sqlWriter, 0, 0);
    System.out.println(sqlWriter.toString());
    System.out.println(builder.getTrailingCondition());

    //
    // sum(entries.quantity)
    // _.orders.entries
    // product.category
    // o.entries
    // parent.time
    // _.orders o JOIN o.entries;

    /*
     * //Can we call the function for each item in the tree?
     * toJoin(Optional<Name> currentRelationContext, ResolvedNamePath namePath);
     *
     *
     *
     * Class: NamePathToJoin
     * SqlNode rel; //check state: tableref, join, call (alised subquery)
     * Optional<ConditionOnRel> condition;
     *
     * //To be placed on the join condition
     * Class: ConditionOnRel
     * Name lhsRelAlias;
     * SqlCall condition;
     *
     *
     * orders.entries := select * from _.product;
     * _.product
     * JOIN Product ON Product.productid = _.productid
     * rel: same, realias underscore to be whatever relation we just came from.
     *
     *
     *
     */

    String aliasJoin = "o.entries e;";
    String resultAlias = "entries$1 e"; // + a condition on o for alias e

    String expr = "sum(entries.quantity)";
    String subqueryPullout =
        "SELECT _uuid, sum(quantity) FROM entries GROUP BY _uuid;"; //sub table type: entries

    String join = "_.orders.entries";
    String result =
        "LEFT JOIN orders o ON _.customerid = o.customerid LEFT JOIN entries e ON e._uuid = o._uuid";

    //Remove field, pass to join thingy, returns last alias
    String inline = "product.category";
    String resultA = "LEFT JOIN product p ON e.productid = p.productid";
    String tableAlias = "p";


    String expr2 = "sum(Orders.entries.quantity)";
    String subqueryPullout2 =
        "SELECT sum(quantity) FROM Orders.entries;"; //sub table type: entries
//
//    String expr3 = "sum(entries.quantity) * sum(entries.unit_price)";
//    Map<String, String> subqueryPullout3 =
//        // + join criteria (type entries, at a specific table)
//        Map.of(
//           "sum(entries.quantity)", "SELECT _uuid, sum(quantity) FROM entries GROUP BY _uuid;",
//            "sum(entries.unit_price)", "SELECT _uuid, sum(unit_price) FROM entries GROUP BY _uuid;"
//        );
  }
}