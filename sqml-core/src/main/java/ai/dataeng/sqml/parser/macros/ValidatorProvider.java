//package ai.dataeng.sqml.parser.macros;
//
//import ai.dataeng.sqml.parser.SqrlToSqlParser;
//import ai.dataeng.sqml.planner.Planner;
//import ai.dataeng.sqml.schema.Namespace;
//import ai.dataeng.sqml.tree.name.NamePath;
//import java.util.Optional;
//import org.apache.calcite.sql.validate.SqlValidator;
//
//public class ValidatorProvider {
//
//  private final SqrlToSqlParser planner;
//  private final Optional<NamePath> tableName;
//  private final Namespace namespace;
//
//  public ValidatorProvider(SqrlToSqlParser sqrlToSqlParser,
//      Optional<NamePath> tableName, Namespace namespace) {
//
//    this.planner = sqrlToSqlParser;
//    this.tableName = tableName;
//    this.namespace = namespace;
//  }
//
//  public SqlValidator create() {
//    return planner.getValidator(tableName, namespace);
//  }
//}
