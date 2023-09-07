//package com.datasqrl.calcite;
//
//import com.datasqrl.calcite.function.vector.TextSearch;
//import com.datasqrl.flink.FlinkConverter;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
//import org.apache.calcite.rex.RexBuilder;
//import org.apache.calcite.sql.*;
//import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
//import org.apache.calcite.sql.fun.SqlStdOperatorTable;
//import org.apache.calcite.sql.type.SqlTypeName;
//import org.apache.calcite.tools.RelBuilder;
//import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.testcontainers.containers.PostgreSQLContainer;
//import org.testcontainers.utility.DockerImageName;
//
//import java.util.Optional;
//
//
//class RelToSqlTransformTest {
//  static DockerImageName image = DockerImageName.parse("postgres:14.2");
//  static PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer(image);
//
//  private SqrlFramework framework;
//  private RelBuilder relBuilder;
//  private FlinkConverter flinkConverter;
//  private RexBuilder rexBuilder;
//  private RelToSqlConverter converter;
//
//  @BeforeAll
//  public static void before() {
//    postgreSQLContainer.start();
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
//
//  }
//
//  @Test
//  public void whereTest() {
//    SqlFunction fnc = flinkConverter.convertFunction("textsearch", "textsearch",
//        new TextSearch(), Optional.empty());
//    this.framework.getSqrlOperatorTable().addFunction("textsearch", fnc);
//
//    RelNode relNode = relBuilder.scan("ENTRIES$")
//        .filter(
//            relBuilder.call(SqlStdOperatorTable.GREATER_THAN,
//                relBuilder.call(fnc,
//                 rexBuilder.makeLiteral("query"), rexBuilder.makeLiteral("param1")),
//                    rexBuilder.makeZeroLiteral(framework.getTypeFactory().createSqlType(SqlTypeName.BIGINT))))
//        .build();
//
//    SqlNode node = this.framework.getQueryPlanner()
//        .relToSql(Dialect.POSTGRES, relNode);
//
//    System.out.println(node.toSqlString(PostgresqlSqlDialect.DEFAULT));
//  }
//
//  @Test
//  public void orderTest() {
//    SqlFunction fnc = flinkConverter.convertFunction("textsearch", "textsearch",
//        new TextSearch(), Optional.empty());
//    this.framework.getSqrlOperatorTable().addFunction("textsearch", fnc);
//
//    SqlNode query = framework.getQueryPlanner().parse(Dialect.CALCITE,
//        "SELECT textsearch('query', 'param1') FROM ENTRIES$ ORDER BY 1");
//
//    RelNode relNode = framework.getQueryPlanner().plan(Dialect.CALCITE, query);
//
//    relNode = this.framework.getQueryPlanner()
//        .convertRelToDialect(Dialect.POSTGRES, relNode);
//
//    SqlNode node = this.framework.getQueryPlanner()
//        .relToSql(Dialect.POSTGRES, relNode);
//
//    System.out.println(node.toSqlString(PostgresqlSqlDialect.DEFAULT));
//  }
//}