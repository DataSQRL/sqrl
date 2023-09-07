//import com.datasqrl.calcite.CalciteTestUtil;
//import com.datasqrl.calcite.Dialect;
//import com.datasqrl.calcite.QueryPlanner;
//import com.datasqrl.calcite.SqrlFramework;
//import com.datasqrl.flink.FlinkConverter;
//import com.datasqrl.function.SqrlFunction;
//import com.datasqrl.function.StdVectorLibraryImpl;
//import com.datasqrl.util.ResultSetPrinter;
//import lombok.SneakyThrows;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
//import org.apache.calcite.rex.RexBuilder;
//import org.apache.calcite.sql.SqlFunction;
//import org.apache.calcite.sql.SqlNode;
//import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
//import org.apache.calcite.tools.RelBuilder;
//import org.apache.flink.table.functions.FunctionDefinition;
//import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.testcontainers.containers.PostgreSQLContainer;
//import org.testcontainers.utility.DockerImageName;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.ResultSet;
//import java.util.List;
//import java.util.Optional;
//
//public class VectorTest {
//  static DockerImageName image = DockerImageName.parse("ankane/pgvector:v0.4.4")
//      .asCompatibleSubstituteFor("postgres");
//  static PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer(image);
//
//  private SqrlFramework framework;
//  private RelBuilder relBuilder;
//  private FlinkConverter flinkConverter;
//  private RexBuilder rexBuilder;
//  private RelToSqlConverter converter;
//  private QueryPlanner planner;
//
//  @BeforeAll
//  public static void before() {
//    postgreSQLContainer.start();
//    populate();
//  }
//
//  @SneakyThrows
//  private static void populate() {
//    connection().createStatement().execute("CREATE EXTENSION vector");
//    connection().createStatement()
//        .execute("CREATE TABLE ENTRIES$ (_uuid varchar, discount int, productid int, embedding vector(3))");
//    connection().createStatement()
//        .execute("INSERT INTO ENTRIES$ (_uuid, discount, productid, embedding) VALUES ('a', 0, 0, '[1,2,3]')");
//    connection().createStatement()
//        .execute("INSERT INTO ENTRIES$ (_uuid, discount, productid, embedding) VALUES ('x', 0, 0, '[4,5,6]')");
//  }
//
//  @AfterAll
//  public static void stop() {
//    postgreSQLContainer.stop();
//  }
//
//  @BeforeEach
//  public void setup() {
//    this.framework = CalciteTestUtil.createEcommerceFramework();
//    this.relBuilder = framework.getQueryPlanner().getRelBuilder();
//    this.rexBuilder = framework.getQueryPlanner().getRexBuilder();
//    this.flinkConverter = new FlinkConverter(
//        framework.getQueryPlanner().getRexBuilder(),
//        framework.getQueryPlanner().getTypeFactory());
//    converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
//    this.planner = framework.getQueryPlanner();
//    for (SqrlFunction function : StdVectorLibraryImpl.SQRL_FUNCTIONS) {
//      SqlFunction fnc = flinkConverter.convertFunction(function.getFunctionName().getCanonical(),
//          function.getFunctionName().getCanonical(),
//          (FunctionDefinition) function, Optional.empty());
//      this.framework.getSqrlOperatorTable().addFunction(function.getFunctionName().getCanonical(),
//          fnc);
//    }
//  }
//
//  @SneakyThrows
//  public static Connection connection() {
//    String jdbcUrl = postgreSQLContainer.getJdbcUrl();
//    String username = postgreSQLContainer.getUsername();
//    String password = postgreSQLContainer.getPassword();
//
//    Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
//    return conn;
//  }
//
//  @SneakyThrows
//  @Test
//  public void testVectorCalcite() {
//    SqlNode query = framework.getQueryPlanner().parse(Dialect.CALCITE,
//        "SELECT cosineDistance(embedding, ARRAY[4.0,2.0,1.0]), " +
//            "cosineSimilarity(embedding, ARRAY[4.0,2.0,1.0])," +
//            "EuclideanDistance(embedding, ARRAY[4.0,2.0,1.0]) FROM ENTRIES$");
//
//    RelNode relNode = planner.plan(Dialect.CALCITE, query);
//    relNode = planner.convertRelToDialect(Dialect.POSTGRES, relNode);
//    SqlNode node = planner.relToSql(Dialect.POSTGRES, relNode);
//    String q = planner.sqlToString(Dialect.POSTGRES, node)
//      .replaceAll("\"", "")
//        //todo types
//      .replaceAll("DOUBLE ARRAY", "vector");
//
//    System.out.println(q);
//
//    ResultSet resultSet = connection().createStatement()
//        .executeQuery(q);
//
//    System.out.println(ResultSetPrinter.toString(resultSet));
//  }
//
//  @SneakyThrows
//  @Test
//  public void testVectorSqrl() {
////    planner.getSchemaMetadata().getNodeMapping().put(List.of("Product"),
////        planner.parse(Dialect.CALCITE, "SELECT * FROM ENTRIES$"));
//    SqlNode query = framework.getQueryPlanner().parse(Dialect.SQRL,
//        "X := SELECT cosineDistance(embedding, embedding), " +
//            "cosineSimilarity(embedding, embedding)," +
//            "EuclideanDistance(embedding, embedding) FROM Product");
//
//    RelNode relNode = planner.plan(Dialect.SQRL, query);
//    relNode = planner.convertRelToDialect(Dialect.POSTGRES, relNode.getInput(0).getInput(0));
//    SqlNode node = planner.relToSql(Dialect.POSTGRES, relNode);
//    String q = planner.sqlToString(Dialect.POSTGRES, node)
//        .replaceAll("\"", "")
//        //todo types
//        .replaceAll("DOUBLE ARRAY", "vector");
//
//    System.out.println(q);
//
//    ResultSet resultSet = connection().createStatement()
//        .executeQuery(q);
//
//    System.out.println(ResultSetPrinter.toString(resultSet));
//  }
//
//}
