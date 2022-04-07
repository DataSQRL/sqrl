package ai.dataeng.sqml.parser.sqrl;

import ai.dataeng.sqml.parser.SqlNodeToFieldMapper;
import ai.dataeng.sqml.parser.sqrl.operations.SqrlOperation;
import ai.dataeng.sqml.schema.Namespace;
import ai.dataeng.sqml.schema.SchemaUpdater;
import ai.dataeng.sqml.tree.SqrlStatement;
import java.util.List;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class QueryProcessor {
  Namespace namespace;

//  SqrlToSqlParser sqrlToSqlParser;
  SchemaUpdater schemaUpdater;
  SqlNodeToFieldMapper mapper;

  public List<SqrlOperation> process(SqrlStatement statement) {
//
//    QueryAssignment assignment = (QueryAssignment) statement;
//
//    ParseResult result = sqrlToSqlParser.parseQuery(assignment);
//    List<Field> fields = mapper.mapQuery(result);
//    VersionedName tableName = schemaUpdater.addTable(assignment.getNamePath(), fields);
//    Table table = namespace.lookup(tableName).get();
//
//    SqrlTypeFactory typeFactory = new SqrlTypeFactory();
//    SqrlCalciteCatalogReader catalogReader = CalciteTools.getCalciteCatalogReader(Optional.empty(), namespace, typeFactory);
//    RelOptCluster cluster = CalciteTools.createHepCluster(typeFactory);
//
//    SqlValidator validator = CalciteTools.getValidator(catalogReader, typeFactory, SqrlOperatorTable.instance());
//
//    SqlToRelConverter relConverter = new SqlToRelConverter(
//        (rowType, queryString, schemaPath
//            , viewPath) -> null,
//        validator,
//        catalogReader,
//        cluster,
//        StandardConvertletTable.INSTANCE,
//        SqlToRelConverter.config());
//
//    validator.validate(result.getSqlNode());
//    RelNode node = relConverter.convertQuery(result.getSqlNode(), false, true).rel;
//
//
//    System.out.println(node.explain());
//
//    RelNode attached = node.accept(CalciteTools.getAssembler(namespace));
//    System.out.println(attached.explain());
//    table.setRelNode(attached);

    return List.of();
  }
}
