package com.datasqrl.calcite;

import com.datasqrl.calcite.convert.SchemaMetadata;
import com.datasqrl.calcite.convert.SqrlToSqlConverter;
import com.datasqrl.calcite.function.builtin.MyCosineDistance;
import com.datasqrl.calcite.function.builtin.MySimpleVector;
import com.datasqrl.calcite.function.builtin.NOW;
import com.datasqrl.calcite.testTables.EntriesTable;
import com.datasqrl.calcite.testTables.Orders;
import com.datasqrl.calcite.testTables.Product;
import com.datasqrl.calcite.type.MyVectorType;
import com.datasqrl.flink.FlinkConverter;
import com.datasqrl.util.DataContextImpl;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.flink.table.api.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.xml.validation.Schema;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

class SqrlPlannerTest {

  QueryPlanner planner;
  FlinkConverter flinkConverter;
  SqrlFramework framework;
  SchemaMetadata schemaMetadata;

  @BeforeEach
  public void before() {
    framework = new SqrlFramework();
    planner = framework.getQueryPlanner();
    flinkConverter = new FlinkConverter(planner.createRexBuilder(), planner.getTypeFactory());

    framework.getSchema()
        .add("ENTRIES$", new EntriesTable());

    framework.getSchema()
        .add("ORDERS$", new Orders());

    framework.getSchema()
        .add("PRODUCT$", new Product());

    SqlFunction function = flinkConverter
        .convertFunction("myFnc","NOW", new NOW());

    SqlFunction myVector = flinkConverter
        .convertFunction("MyVector","MyVector", new MySimpleVector());

    SqlFunction myCosineDistance = flinkConverter
        .convertFunction("MyCosineDistance","MyCosineDistance", new MyCosineDistance());

    RelDataType myVectorType = flinkConverter
        .convertType(DataTypes.of(MyVectorType.class));

    framework.getSqrlOperatorTable()
        .addFunction("myFnc", function);

    framework.getSqrlOperatorTable()
        .addFunction("MyVector", myVector);

    framework.getSqrlOperatorTable()
        .addFunction("MyCosineDistance", myCosineDistance);

    framework.getTypeFactory()
        .registerType(myVectorType);

    //schema mappings
    Map<List<String>, SqlNode> nodeMapping = new HashMap<>();
    Map<List<String>, List<String>> joinMapping = new HashMap<>();
    Map<List<String>, List<String>> tableArgPositions = new HashMap<>();
    joinMapping.put(List.of("ORDERS", "ENTRIES", "PRODUCT"), List.of("PRODUCT"));
    nodeMapping.put(List.of("ORDERS", "ENTRIES"), planner.parse(Dialect.SQRL,
        "SELECT e.* " +
            "FROM entries$ AS e " +
            "WHERE @._uuid = e._uuid"));
    nodeMapping.put(List.of("ORDERS"), planner.parse(Dialect.SQRL,
        "SELECT * " +
            "FROM orders$"));
    nodeMapping.put(List.of("ORDERS", "ENTRIES", "PRODUCT"), planner.parse(Dialect.SQRL,
        "SELECT p.* " +
            "FROM product$ AS p " +
            "WHERE @.productid = p.id AND id > ?"));
    nodeMapping.put(List.of("PRODUCT"), planner.parse(Dialect.SQRL,
        "SELECT * " +
            "FROM product$"));
    tableArgPositions.put(List.of("ORDERS", "ENTRIES"), List.of("_UUID"));
    tableArgPositions.put(List.of("ORDERS", "ENTRIES", "PRODUCT"), List.of("PRODUCTID"));

    this.schemaMetadata = new SchemaMetadata(nodeMapping, joinMapping, tableArgPositions);

  }

  @Test
  public void testSqrlQuery() throws Exception {
    executeQuery(
        //assume
        // Orders.entries.product(id) := JOIN Products p on p.id = @.productid AND id > :id
        "SELECT e._uuid, p.id, p.name, \n" +
            "       MyCosineDistance(MyVector('jim'), MyVector(p.name)) AS cosine \n" +
            "FROM Orders.entries e \n" +
            "JOIN e.product(100) p \n" +
            "ORDER BY cosine DESC");
  }

  public void executeQuery(String sql) {
    SqlNode parsedSqlNode = planner.parse(Dialect.SQRL, sql);
    parsedSqlNode = SqrlToSqlConverter.convertToSql(parsedSqlNode, schemaMetadata);
//    Assure reparsable
    planner.parse(Dialect.CALCITE, parsedSqlNode.toSqlString(CalciteSqlDialect.DEFAULT).toString());

    RelNode relNode = planner.plan(parsedSqlNode);
//    System.out.println(relNode.explain());
//    System.out.println(planner.toSql(Dialect.CALCITE, relNode));

    EnumerableRel enumerableRel = planner.convertToEnumerableRel(relNode);

    Enumerator<Object[]> enumerator = planner.execute(enumerableRel,
        new DataContextImpl(this.framework));

    while (enumerator.moveNext()) {
      System.out.println(Arrays.toString(enumerator.current()));
    }
    enumerator.close();
  }
}