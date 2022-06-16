//package ai.datasqrl.plan.local.validateSql;
//
//import ai.datasqrl.parse.tree.AstVisitor;
//import ai.datasqrl.parse.tree.ImportDefinition;
//import ai.datasqrl.parse.tree.JoinDeclaration;
//import ai.datasqrl.parse.tree.LongLiteral;
//import ai.datasqrl.parse.tree.Node;
//import ai.datasqrl.parse.tree.QuerySpecification;
//import ai.datasqrl.parse.tree.Select;
//import ai.datasqrl.parse.tree.SingleColumn;
//import ai.datasqrl.plan.calcite.CalciteEnvironment;
//import ai.datasqrl.plan.calcite.CalcitePlanner;
//import ai.datasqrl.plan.local.transpiler.toSql.ConvertContext;
//import ai.datasqrl.plan.local.transpiler.toSql.SqlNodeConverter;
//import ai.datasqrl.schema.Schema;
//import ai.datasqrl.plan.local.transpiler.toSql.SqlNodeFormatter;
//import java.util.List;
//import java.util.Optional;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.calcite.schema.SqrlCalciteSchema;
//import org.apache.calcite.sql.SqlNode;
//import org.apache.calcite.sql.validate.SqlValidator;
//
//@Slf4j
//public class ValidateSql extends AstVisitor<SqlResult, Void> {
//
//  private final CalcitePlanner planner;
//
//  public ValidateSql(CalcitePlanner planner) {
//
//    this.planner = planner;
//  }
//
//  public static Optional<SqlResult> validate(CalciteEnvironment calcite, Optional<Node> node, Schema schema) {
//    if (node.isEmpty()) {
//      return Optional.empty();
//    }
//    ValidateSql validateSql = new ValidateSql(new CalcitePlanner(calcite, new SqrlCalciteSchema(schema)));
//
//    return Optional.ofNullable(node.get().accept(validateSql, null));
//  }
//
//  @Override
//  public SqlResult visitJoinAssignment(JoinDeclaration node, Void context) {
//    QuerySpecification querySpecification = new QuerySpecification(
//        Optional.empty(),
//        new Select(List.of(new SingleColumn(new LongLiteral("1")))),
//        node.getRelation(),
//        Optional.empty(),
//        Optional.empty(),
//        Optional.empty(),
//        node.getOrderBy(),
//        node.getLimit()
//    );
//
//    SqlNodeConverter converter = new SqlNodeConverter();
//    SqlNode sqlNode = querySpecification.accept(converter, new ConvertContext());
//
//
//    SqlValidator validator = planner.createValidator();
//
//    SqlNode validated = validator.validate(sqlNode);
//
//    return new SqlResult(validated, validator);
//  }
//
//  @Override
//  public SqlResult visitNode(Node node, Void context) {
//    SqlNodeConverter converter = new SqlNodeConverter();
//    SqlValidator validator = planner.createValidator();
//    SqlNode sqlNode = node.accept(converter, new ConvertContext());
//    log.info("Pre-validated SqlNode {}:", SqlNodeFormatter.toString(sqlNode));
//
//    SqlNode validated = validator.validate(sqlNode);
//
//    return new SqlResult(validated, validator);
//  }
//
//  @Override
//  public SqlResult visitImportDefinition(ImportDefinition node, Void context) {
//    return null;
//  }
//}
