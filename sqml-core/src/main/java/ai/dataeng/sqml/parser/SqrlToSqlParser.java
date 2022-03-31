package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.parser.macros.SqrlTranslator;
import ai.dataeng.sqml.parser.macros.SqrlTranslator.Scope;
import ai.dataeng.sqml.parser.macros.ValidatorProvider;
import ai.dataeng.sqml.schema.Namespace;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CachingSqrlSchema;
import org.apache.calcite.jdbc.SqrlTypeFactory;
import org.apache.calcite.prepare.SqrlCalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SqrlSchema;
import org.apache.calcite.sql.FlattenTableNames;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqrlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqrlValidator;

@AllArgsConstructor
public class SqrlToSqlParser {
  Namespace namespace;
//
//  public ParseResult parseDistinct(DistinctAssignment assignment) {
//    List<String> partition = assignment.getPartitionKeys().stream()
//        .map(n->String.format("%s", n.getCanonical()))
//        .collect(Collectors.toList());
//
//    String query = SqrlQueries.generateDistinct(assignment, partition);
//
//    ValidatorProvider validatorProvider = new ValidatorProvider(this, assignment.getNamePath().getPrefix(), namespace);
//    SqlValidator validator = validatorProvider.create();
//    SqlNode sqlNode = parse(query);
//    validator.validate(sqlNode);
//    SqrlTranslator translator = new SqrlTranslator(validator, validatorProvider, null, Optional.empty());
//    Scope scope = translator.visitQuery((SqlSelect) sqlNode, null);
//    return new ParseResult(scope.getNode(), partition, translator.getValidator(), Optional.empty(), Map.of());
//  }
//
//  public ParseResult parseExpression(ExpressionAssignment assignment) {
//    System.out.println("Processing: "+ assignment.getSql());
//
//    Optional<NamePath> tableName = assignment.getNamePath().getPrefix();
//    if (tableName.isEmpty()) {
//      throw new RuntimeException("Could not assign expression to root");
//    }
//    Table table = namespace.lookup(tableName.get())
//        .orElseThrow(() -> new RuntimeException("Could not find table"));
//
//    Name columnName = assignment.getNamePath().getLast();
//    String sql = String.format("SELECT %s AS %s FROM _",
//        assignment.getSql(), columnName.toString());
//    SqlNode sqlNode = parse(sql);
//
//    SqlValidator validator = getValidator(tableName, namespace);
//    validator.validate(sqlNode);
//    ValidatorProvider provider = new ValidatorProvider(this, tableName, namespace);
//
//    SqrlTranslator translator = new SqrlTranslator(validator, provider, Optional.of(table));
//    Scope scope = translator.visitQuery((SqlSelect) sqlNode, null);
//    return new ParseResult(scope.getNode(), List.of(), translator.getValidator(), Optional.empty(), Map.of());
//  }
//
//  public ParseResult parseJoin(JoinDeclaration assignment) {
//    System.out.println("Processing Join: " + assignment.getQuery());
//    NamePath context = assignment.getNamePath().getPrefix()
//        .orElseThrow(()->new RuntimeException(String.format("Cannot assign join to prefix %s", assignment.getNamePath())));
//
//    Table table = namespace.lookup(context)
//        .orElseThrow(()->new RuntimeException(String.format("Could not find source table: %s", context)));
//
//    String toTableName = getLastTable(assignment.getInlineJoin().getJoin()).getDisplay();
//
//    Preconditions.checkState(!table.getPrimaryKeys().isEmpty(), table.getFields().getElements());
//    //Add SqlNode to relationship
//    AliasGenerator gen = new AliasGenerator();
//    Map<Column, String> ppkAliases = table.getPrimaryKeys().stream()
//        .collect(Collectors.toMap(e -> e, e -> gen.nextAlias()));
//    String ppkSql = ppkAliases.entrySet().stream()
//        .map(e-> String.format("`_`.`%s` AS %s", e.getKey().getId(), e.getValue()))
//        .collect(Collectors.joining(", "));
//    String template = "SELECT %s, %s.* FROM _ %s";
//    String sql = String.format(template, ppkSql, toTableName, assignment.getQuery());
//
//    ValidatorProvider validatorProvider = new ValidatorProvider(this,
//        assignment.getNamePath().getPrefix(), namespace);
//    SqlNode node = parse(sql);
//    SqlValidator validator = validatorProvider.create();
//
//    validator.validate(node);
//
//    Optional<Table> destinationOptional = FindAliasedTable.findTable(node, toTableName, validator);
//    if (destinationOptional.isEmpty()) {
//      throw new RuntimeException("Could not find table after join");
//    }
//
//    SqrlTranslator translator = new SqrlTranslator(validator, validatorProvider,  Optional.of(table));
//    translator.visit(node, null);
//    if (node instanceof SqlOrderBy) {
//      node = ((SqlOrderBy)node).getOperandList().get(0);
//    }
//
//    SqlSelect select = (SqlSelect) node;
//    List<SqlNode> selectList = new ArrayList<>();
//    selectList.addAll(select.getSelectList().getList().subList(0, table.getPrimaryKeys().size()));
//    selectList.add(new SqlIdentifier(List.of(toTableName, ""), SqlParserPos.ZERO));
//    select.setSelectList(new SqlNodeList(selectList, SqlParserPos.ZERO));
//
//    return new ParseResult(select, List.of(), translator.getValidator(), destinationOptional, ppkAliases);
//  }
//
//
//  private Name getLastTable(InlineJoinBody join) {
//    if (join.getInlineJoinBody().isPresent()) {
//      return getLastTable(join.getInlineJoinBody().get());
//    }
//    if (join.getAlias().isPresent()) {
//      return Name.system(join.getAlias().get().getValue());
//    }
//
//    Preconditions.checkState(join.getTable().getLength() == 1, "Alias must be present on pathed joins");
//
//    return join.getTable().getFirst();
//  }

  public ParseResult parseQuery(QueryAssignment assignment) {
    System.out.println("Query: " + assignment.getSql());
    Optional<Table> table = assignment.getNamePath().getPrefix()
        .flatMap(namespace::lookup);

    ValidatorProvider validatorProvider = new ValidatorProvider(this, assignment.getNamePath().getPrefix(), namespace);
    SqlValidator validator = validatorProvider.create();

    SqrlTranslator translator = new SqrlTranslator(validator, validatorProvider, table);

    SqlNode node = parse(assignment.getSql());
    validator.validate(node);

    if (node instanceof SqlOrderBy) {
      node = ((SqlOrderBy)node).getOperandList().get(0);
    }

    Scope scope = translator.visit(node, null);

    return new ParseResult(scope.getNode(), List.of(), translator.getValidator(), Optional.empty(), Map.of());
  }


  @SneakyThrows
  public SqlNode parse(String sql) {
    org.apache.calcite.sql.parser.SqlParser.Config parserConfig = org.apache.calcite.sql.parser.SqlParser.config()
        .withParserFactory(org.apache.calcite.sql.parser.SqlParser.config().parserFactory())
        .withLex(Lex.JAVA)
        .withConformance(SqlConformanceEnum.LENIENT)
        .withIdentifierMaxLength(256);
    org.apache.calcite.sql.parser.SqlParser parser = SqlParser.create(sql, parserConfig);
    SqlNode sqlNode = parser.parseQuery();

    // Validate the initial AST
    FlattenTableNames.rewrite(sqlNode);

    return sqlNode;
  }

  public SqlValidator getValidator(Optional<NamePath> context, Namespace namespace) {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory();
    SqrlCalciteCatalogReader catalogReader = getCalciteCatalogReader(context, namespace, typeFactory);
    SqlValidator validator = getValidator(catalogReader, typeFactory, SqrlOperatorTable.instance());
    return validator;
  }

  private SqlValidator getValidator(SqrlCalciteCatalogReader catalogReader,
      RelDataTypeFactory typeFactory, ReflectiveSqlOperatorTable operatorTable) {
    SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
        .withCallRewrite(true)
        .withIdentifierExpansion(true)
        .withColumnReferenceExpansion(true)
        .withLenientOperatorLookup(true)
        .withSqlConformance(SqlConformanceEnum.LENIENT)
        ;

    SqlValidator validator = new SqrlValidator(
        operatorTable,
        catalogReader,
        typeFactory,
        validatorConfig);
    return validator;
  }

  private SqrlCalciteCatalogReader getCalciteCatalogReader(Optional<NamePath> context,
      Namespace namespace, SqrlTypeFactory typeFactory) {
    CalciteTableFactory calciteTableFactory = new CalciteTableFactory(context, namespace, new RelDataTypeFieldFactory(typeFactory), true);
    CachingSqrlSchema schema = new CachingSqrlSchema(new SqrlSchema(calciteTableFactory));

    // Configure and instantiate validator
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    SqrlCalciteCatalogReader catalogReader = new SqrlCalciteCatalogReader(schema,
        Collections.singletonList(""),
        typeFactory, config);
    return catalogReader;
  }
}
