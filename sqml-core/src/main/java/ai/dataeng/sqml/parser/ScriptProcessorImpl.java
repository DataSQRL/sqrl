package ai.dataeng.sqml.parser;

import static ai.dataeng.sqml.planner.macros.SqlNodeUtils.childParentJoin;
import static ai.dataeng.sqml.planner.macros.SqlNodeUtils.parentChildJoin;
import static ai.dataeng.sqml.tree.name.Name.PARENT_RELATIONSHIP;

import ai.dataeng.sqml.config.provider.HeuristicPlannerProvider;
import ai.dataeng.sqml.planner.AliasGenerator;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Dataset;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.FindAliasedTable;
import ai.dataeng.sqml.planner.Planner;
import ai.dataeng.sqml.planner.Relationship;
import ai.dataeng.sqml.planner.Relationship.Multiplicity;
import ai.dataeng.sqml.planner.Relationship.Type;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.planner.macros.FieldFactory;
import ai.dataeng.sqml.planner.macros.SqrlTranslator;
import ai.dataeng.sqml.planner.macros.SqrlTranslator.Scope;
import ai.dataeng.sqml.planner.macros.ValidatorProvider;
import ai.dataeng.sqml.planner.operator.ImportResolver;
import ai.dataeng.sqml.planner.operator.ImportResolver.ImportMode;
import ai.dataeng.sqml.schema.Namespace;
import ai.dataeng.sqml.tree.CreateSubscription;
import ai.dataeng.sqml.tree.DistinctAssignment;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.InlineJoinBody;
import ai.dataeng.sqml.tree.JoinDeclaration;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.ScriptNode;
import ai.dataeng.sqml.tree.SortItem.Ordering;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.config.error.ErrorMessage;
import ai.dataeng.sqml.config.error.ErrorCollector;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor
public class ScriptProcessorImpl implements ScriptProcessor {
  ImportResolver importResolver;
  HeuristicPlannerProvider plannerProvider;
  Namespace namespace;

  public Namespace process(ScriptNode script) {

    for (Node statement : script.getStatements()) {
      if (statement instanceof ImportDefinition) {
        process((ImportDefinition) statement, namespace);
      } else if (statement instanceof QueryAssignment) {
        process((QueryAssignment) statement, namespace);
      } else if (statement instanceof ExpressionAssignment) {
        process((ExpressionAssignment) statement, namespace);
      } else if (statement instanceof JoinDeclaration) {
        process((JoinDeclaration) statement, namespace);
      } else if (statement instanceof DistinctAssignment) {
        process((DistinctAssignment) statement, namespace);
      } else if (statement instanceof CreateSubscription) {
        process((CreateSubscription) statement, namespace);
      } else {
        throw new RuntimeException(String.format("Unknown statement type %s", statement.getClass().getName()));
      }
    }

    return namespace;
  }

  @SneakyThrows
  public void process(ImportDefinition statement, Namespace namespace) {
    NamePath name = statement.getNamePath();
    ErrorCollector errors = ErrorCollector.root();

    if (name.getLength() > 2) {
      throw new RuntimeException(String.format("Cannot import identifier: %s", name));
    }
    if (name.getLength() == 1) {
      importResolver.resolveImport(ImportMode.DATASET, name.getFirst(), Optional.empty(),
          statement.getAliasName(), namespace, errors);

    } else if (name.get(1).getDisplay().equals("*")) {
      if (statement.getAliasName().isPresent()) {
        throw new RuntimeException(String.format("Could not alias star (*) import: %s", name));
      }
      //adds to ns
      importResolver.resolveImport(ImportMode.ALLTABLE, name.getFirst(), Optional.empty(),
          Optional.empty(), namespace, errors);

    } else {
      //adds to ns
      importResolver.resolveImport(ImportMode.TABLE, name.getFirst(), Optional.of(name.get(1)),
          statement.getAliasName(), namespace, errors);
    }

    //TODO: Error manager
    if (errors.isFatal()) {
      throw new RuntimeException(String.format("Import errors: %s", errors));
    }
  }

  /**
   * Distinct a table based on a key and sort.
   */
  public void process(DistinctAssignment statement, Namespace namespace) {
    System.out.println("Processing distinct:" + statement.toString());
    AliasGenerator gen = new AliasGenerator();
    //https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/deduplication/
    String template = "SELECT * FROM ("
        + " SELECT *, ROW_NUMBER() OVER (PARTITION BY %s %s) __row_number "
        + " FROM %s) %s "
        + "WHERE __row_number = 1";
    List<String> partition = statement.getPartitionKeys().stream()
        .map(Name::getCanonical)
        .collect(Collectors.toList());
    String partitionStr = String.join(", ", partition);

    List<String> order = statement.getOrder().stream()
        .map(o->o.getSortKey().toString() + o.getOrdering().map(dir->" " + ((dir == Ordering.DESCENDING) ? "DESC" : "ASC")).orElse(""))
        .collect(Collectors.toList());
    String orderStr = order.isEmpty() ? " " : " ORDER BY " + String.join(", ", order);

    String query = String.format(template,
        partitionStr, orderStr,
        statement.getTable().getCanonical(),
        gen.nextTableAlias()
    );

    Planner planner = plannerProvider.createPlanner();
    ValidatorProvider validatorProvider = new ValidatorProvider(planner, statement.getNamePath().getPrefix(), namespace);
    SqlValidator validator = validatorProvider.create();
    SqlNode sqlNode = planner.parse(query);
    validator.validate(sqlNode);
    SqrlTranslator translator = new SqrlTranslator(validator,validatorProvider, planner, Optional.empty());
    Scope scope = translator.visitQuery((SqlSelect) sqlNode, null);
    List<Field> fields = FieldFactory.createFields(translator.getValidator(),
        ((SqlSelect)scope.getNode()).getSelectList(),  ((SqlSelect)scope.getNode()).getGroup(),
        this.namespace.lookup(statement.getTable().toNamePath()));
    Table table = namespace.createTable(
        statement.getNamePath().getLast(),
        statement.getNamePath(),
        false);

    for (Field field : fields) {
      if (field.getName().getCanonical().equals("__row_number")) {
        continue;
      }
      if (partition.contains(field.getName().getCanonical().split("\\$")[0])) {
        ((Column) field).setPrimaryKey(true);
      }
      table.addField(field);
    }

    System.out.println("\nFINAL Query :\n" + sqlNode);
  }

  /**
   * Expression assignments adds a column, potentially shadowed, to the context relation. Expression
   * assignments are difficult to parse with calcite b/c the proper grouping expressions are
   * invalid. We need to override the validator to allow them to be accepted as an aggregate.
   * For example: `sum(total) + baseCost` would be an invalid transformation of select ... from _
   * b/c the baseCost should have been added as a grouping parameter. We solve this though a bit of
   * a hack: we relax the validator to accept grouping clauses that may be out of bounds and then
   * check later.
   *
   * Shadowed fields don't need to stick around but we keep them around anyway for logical plan
   * optimization.
   *
   * Relationships cannot be inherited.
   *
   * A table's columns could be lazier. We don't need to convert to sqrl types until we need to move
   * to a graphql implementation. We do however need to track types in granular detail. We will
   * frequently want to walk table relationships. This is a good instance where conversion between
   * the two models is very involved.
   *
   */
  public void process(ExpressionAssignment expr, Namespace namespace) {
    System.out.println("Processing: "+ expr.getSql());

    Optional<NamePath> tableName = expr.getNamePath().getPrefix();
    if (tableName.isEmpty()) {
      throw new RuntimeException("Could not assign expression to root");
    }
    Table table = namespace.lookup(tableName.get())
        .orElseThrow(() -> new RuntimeException("Could not find table"));

    Name columnName = expr.getNamePath().getLast();
    String sql = String.format("SELECT %s AS %s FROM _",
        expr.getSql(), columnName.toString());
    Planner planner = plannerProvider.createPlanner();
    SqlNode sqlNode = planner.parse(sql);

    SqlValidator validator = planner.getValidator(tableName, namespace);
    validator.validate(sqlNode);
    ValidatorProvider provider = new ValidatorProvider(planner, tableName, namespace);

    SqrlTranslator translator = new SqrlTranslator(validator, provider, planner, Optional.of(table));
    Scope scope = translator.visitQuery((SqlSelect) sqlNode, null);
    List<Field> fields = FieldFactory.createFields(translator.getValidator(),
        ((SqlSelect)scope.getNode()).getSelectList(),  ((SqlSelect)scope.getNode()).getGroup(),
        Optional.of(table));

    Optional<Field> field = fields.stream()
        .filter(f->f.getName().getCanonical().equals(columnName.getCanonical()))
        .findFirst();
    if (field.isEmpty()) {
      throw new RuntimeException("Internal error: Could not find field in expression");
    }
    Field f = field.get();

    if (table.getField(f.getName()) != null) {
      ((Column)f).setVersion(table.getField(f.getName()).getVersion() + 1);
    }

    table.addField(f);

    System.out.println("\nFINAL Expression: " + sqlNode + "\n");
    namespace.addToDag(sqlNode);
  }

  /**
   * A statement that expresses a join from the current context to another table.
   *
   * Join declarations with a LIMIT in a context statement:
   *   For example Orders.firstEntry := JOIN _.entries e LIMIT 1;
   *  We put a row_number over (context PK) and filter inside the first JOIN
   *   SELECT e.* FROM (SELECT entires.* FROM )
   *
   *
   *
   */
  public void process(JoinDeclaration statement, Namespace namespace) {
    System.out.println("Processing Join: " + statement.getQuery());
    NamePath context = statement.getNamePath().getPrefix()
        .orElseThrow(()->new RuntimeException(String.format("Cannot assign join to prefix %s", statement.getNamePath())));

    Table table = namespace.lookup(context)
        .orElseThrow(()->new RuntimeException(String.format("Could not find source table: %s", context)));

    String toTableName = getLastTable(statement.getInlineJoin().getJoin()).getDisplay();

    Preconditions.checkState(!table.getPrimaryKeys().isEmpty(), table.getFields().getElements());
    //Add SqlNode to relationship
    AliasGenerator gen = new AliasGenerator();
    Map<Column, String> ppkAliases = table.getPrimaryKeys().stream()
          .collect(Collectors.toMap(e -> e, e -> gen.nextAlias()));
    String ppkSql = ppkAliases.entrySet().stream()
          .map(e-> String.format("_.%s AS %s", e.getKey().getId(), e.getValue()))
          .collect(Collectors.joining(", "));
    String template = "SELECT %s, %s.* FROM _ %s";
    String sql = String.format(template, ppkSql, toTableName, statement.getQuery());

    Planner planner = this.plannerProvider.createPlanner();
    ValidatorProvider validatorProvider = new ValidatorProvider(planner,
        statement.getNamePath().getPrefix(), namespace);
    SqlNode node = planner.parse(sql);
    SqlValidator validator = validatorProvider.create();
    validator.validate(node);

    Optional<Table> destinationOptional = FindAliasedTable.findTable(node, toTableName, validator);
    if (destinationOptional.isEmpty()) {
      throw new RuntimeException("Could not find table after join");
    }

    SqrlTranslator translator = new SqrlTranslator(validator, validatorProvider, planner, Optional.of(table));
    translator.visit(node, null);
    if (node instanceof SqlOrderBy) {
      node = ((SqlOrderBy)node).getOperandList().get(0);
    }

    SqlSelect select = (SqlSelect) node;
    List<SqlNode> selectList = new ArrayList<>();
    selectList.addAll(select.getSelectList().getList().subList(0, table.getPrimaryKeys().size()));
    selectList.add(new SqlIdentifier(List.of(toTableName, ""), SqlParserPos.ZERO));
    select.setSelectList(new SqlNodeList(selectList, SqlParserPos.ZERO));


    Table destination = destinationOptional.get();

    Multiplicity multiplicity = Multiplicity.MANY;
    //TODO:
    if (statement.getInlineJoin().getLimit().isPresent() &&
      statement.getInlineJoin().getLimit().get() == 1) {
      multiplicity = Multiplicity.ONE;
    }

    Relationship rel = new Relationship(statement.getNamePath().getLast(),
        table, destination, Type.JOIN, multiplicity, ppkAliases);

    rel.setSqlNode(node);
    table.addField(rel);


    System.out.println("\nFINAL Join :\n" + node);

//      AliasGenerator gen = new AliasGenerator();
//      Map<String, Column> ppkAliases = table.getPrimaryKeys().stream()
//          .collect(Collectors.toMap(e -> gen.nextAlias(), e -> e));
//      String rowNumAlias = gen.nextAlias();
//      String toTableFieldAlias = gen.nextAlias();
//      String tableAlias = gen.nextTableAlias();
//
//      String ppkSql = ppkAliases.entrySet().stream()
//          .map(e-> String.format("_.%s AS %s", e.getValue().getId(), e.getKey()))
//          .collect(Collectors.joining(", "));
//      String partitionSql = ppkAliases.entrySet().stream()
//          .map(e-> String.format("_.%s", e.getValue().getId(), e.getKey()))
//          .collect(Collectors.joining(", "));
//
//      String sql = "SELECT * FROM \n"
//          + "(\n"
//          + "  SELECT %s, \n"
//          + "         ROW_NUMBER() OVER (PARTITION BY %s) AS %s, \n" //todo: order by
//          + "         %s._uuid AS %s \n"
//          + "  FROM _ %s\n"
//          + ") %s \n"
//          + "WHERE %s <= %d";
//      String formattedSql = String.format(sql, ppkSql,
//          partitionSql, rowNumAlias,
//          toTableName, toTableFieldAlias,
//          statement.getQuery(),
//          tableAlias,
//          rowNumAlias, statement.getInlineJoin().getLimit().get()
//      );
//
  }

  private Multiplicity getMultiplicity(RelNode root) {
    if (root instanceof Sort) {
      Sort sort = (Sort) root;
      if (sort.fetch instanceof RexLiteral) {
        RexLiteral fetch = (RexLiteral) sort.fetch;
        if (((BigDecimal)fetch.getValue()).intValue() == 1) {
          return Multiplicity.ZERO_ONE;
        }
      }
    }

    return Multiplicity.MANY;
  }

  private Name getLastTable(InlineJoinBody join) {
    if (join.getInlineJoinBody().isPresent()) {
      return getLastTable(join.getInlineJoinBody().get());
    }
    if (join.getAlias().isPresent()) {
      return Name.system(join.getAlias().get().getValue());
    }

    Preconditions.checkState(join.getTable().getLength() == 1, "Alias must be present on pathed joins");

    return join.getTable().getFirst();
  }

  public void process(QueryAssignment statement, Namespace namespace) {
    System.out.println("Query: " + statement.getSql());
    Planner planner = plannerProvider.createPlanner();
    Optional<Table> table = statement.getNamePath().getPrefix()
        .flatMap(namespace::lookup);

    ValidatorProvider validatorProvider = new ValidatorProvider(planner, statement.getNamePath().getPrefix(), namespace);
    SqlValidator validator = validatorProvider.create();
    SqrlTranslator translator = new SqrlTranslator(validator, validatorProvider, planner, table);

    SqlNode node = planner.parse(statement.getSql());
    validator.validate(node);

    if (node instanceof SqlOrderBy) {
      node = ((SqlOrderBy)node).getOperandList().get(0);
    }


    Scope scope = translator.visit(node, null);
    List<Field> fields = FieldFactory.createFields(translator.getValidator(),
        ((SqlSelect)scope.getNode()).getSelectList(),  ((SqlSelect)scope.getNode()).getGroup(),
        table);
    //todo: internal fields

    Table destination = namespace.createTable(
        statement.getNamePath().getLast(),
        statement.getNamePath(),
        false);
    fields.forEach(destination::addField);

    //Assignment to root
    if (statement.getNamePath().getPrefix().isEmpty()) {
      List<Table> tables = new ArrayList<>();
      tables.add(destination);
      Dataset rootTable = new Dataset(Dataset.ROOT_NAMESPACE_NAME, tables);
      namespace.addDataset(rootTable);
    } else {
      Table source =
          namespace.lookup(statement.getNamePath().getPrefix().orElseThrow())
              .orElseThrow(()->new RuntimeException(String.format("Could not find table on field %s", statement.getNamePath())));
      Relationship fromField = new Relationship(statement.getNamePath().getLast(),
          source, destination, Type.JOIN, Multiplicity.MANY, null);
      Pair<Map<Column, String>, SqlNode> child = parentChildJoin(fromField);

      fromField.setSqlNode(child.getRight());
      fromField.setPkNameMapping(child.getLeft());

      source.addField(fromField);

      Relationship toField = new Relationship(PARENT_RELATIONSHIP, destination, source, Type.PARENT,
          Multiplicity.ONE,
          null);
      Pair<Map<Column, String>, SqlNode> parent = childParentJoin(toField);
      toField.setSqlNode(parent.getRight());
      toField.setPkNameMapping(parent.getLeft());
      destination.addField(toField);
    }

    System.out.println("\nFINAL Query :\n" + node);
  }

  public void process(CreateSubscription statement, Namespace namespace) {

  }
}
