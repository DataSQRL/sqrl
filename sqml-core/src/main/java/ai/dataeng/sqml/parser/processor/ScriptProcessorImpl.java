package ai.dataeng.sqml.parser.processor;

import static ai.dataeng.sqml.tree.name.Name.PARENT_RELATIONSHIP;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.config.provider.HeuristicPlannerProvider;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Dataset;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.FindAliasedTable;
import ai.dataeng.sqml.planner.Planner;
import ai.dataeng.sqml.planner.PlannerResult;
import ai.dataeng.sqml.planner.RelToSql;
import ai.dataeng.sqml.planner.Relationship;
import ai.dataeng.sqml.planner.Relationship.Multiplicity;
import ai.dataeng.sqml.planner.Relationship.Type;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.planner.AliasGenerator;
import ai.dataeng.sqml.planner.operator.ImportResolver;
import ai.dataeng.sqml.planner.operator.ImportResolver.ImportMode;
import ai.dataeng.sqml.planner.operator2.SqrlRelNode;
import ai.dataeng.sqml.tree.CreateSubscription;
import ai.dataeng.sqml.tree.DistinctAssignment;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.InlineJoinBody;
import ai.dataeng.sqml.tree.JoinAssignment;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.ScriptNode;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import ai.dataeng.sqml.type.basic.ProcessMessage.ProcessBundle;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.OperatorTable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.SqrlRelBuilder;

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
      } else if (statement instanceof JoinAssignment) {
        process((JoinAssignment) statement, namespace);
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
    ProcessBundle<ProcessMessage> errors = new ProcessBundle<ProcessMessage> ();

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
    SqrlRelBuilder relBuilder = plannerProvider.createPlanner()
        .getRelBuilder(Optional.empty(), namespace);

    RelNode node = relBuilder
        .scan_base(statement.getTable().toString())
        .distinctOn(
            statement.getPartitionKeys(),
            statement.getOrder())
        .build();

    System.out.println(statement);
    System.out.println("Distinct plan:\n" + node.explain());

    namespace.createTable(
        statement.getNamePath().getLast(),
        statement.getNamePath(),
        (SqrlRelNode) node,
        false);
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

    PlannerResult result = plannerProvider.createPlanner()
        .plan(tableName, namespace, sql);

    System.out.println(String.format("Sql: %s AS %s", expr.getSql(), columnName));
    System.out.println("Expression plan:\n" + result.getRoot().explain());

    table.setRelNode((SqrlRelNode) result.getRoot());
    //Update version of new field, add it to table

    Field field = ((SqrlRelNode) result.getRoot()).getFields().get(((SqrlRelNode) result.getRoot()).getFields().size() - 1);
    if (table.getField(field.getName()) != null) {
      ((Column)field).setVersion(table.getField(field.getName()).getVersion() + 1);
    }
    table.addField(field);
  }

  public void process(JoinAssignment statement, Namespace namespace) {
    System.out.println("Processing Join: " + statement.getQuery());
    NamePath namePath = statement.getNamePath().getPrefix()
        .orElseThrow(()->new RuntimeException(String.format("Cannot assign join to prefix %s", statement.getNamePath())));

    Table table = namespace.lookup(namePath)
        .orElseThrow(()->new RuntimeException(String.format("Could not find source table: %s", namePath)));

    String lastTableName = getLastTable(statement.getInlineJoin().getJoin()).getDisplay();
    String sql = String.format("SELECT %s.* FROM _ " + statement.getQuery(), lastTableName);

    PlannerResult rslt =  plannerProvider.createPlanner().plan(statement.getNamePath().getPrefix(), namespace, sql);
    SqlNode sqlNode = rslt.getNode();
    RelNode root = rslt.getRoot();

    Optional<Table> destinationOptional = FindAliasedTable.findTable(sqlNode, lastTableName, rslt.getValidator());
    if (destinationOptional.isEmpty()) {
      throw new RuntimeException("Could not find table after join");
    }
    Table destination = destinationOptional.get();

    Relationship rel = new Relationship(statement.getNamePath().getLast(),
        table, destination, Type.JOIN, getMultiplicity(root), null, root);
    rel.setSqlNode(sqlNode);
    table.addField(rel);

    //TODO: Inverse
    if (statement.getInlineJoin().getInverse().isPresent()) {
      Name inverseName = statement.getInlineJoin().getInverse().get();
      Relationship toField = new Relationship(inverseName,
          destination, table, Type.JOIN, Multiplicity.MANY, rel, null);
      destination.addField(toField);
      rel.setInverse(toField);
    }

    System.out.println("JOIN: " + statement.getQuery());
    System.out.println(RelToSql.convertToSql(root));
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

    PlannerResult result = planner.plan(
        statement.getNamePath().getPrefix(),
        namespace,
        statement.getSql());

    RelNode added = result.getRoot();
    System.out.println("Plan: \n" + result.getRoot().explain());

    System.out.println("SQL: "+RelToSql.convertToSql(added));

    List<Field> fieldList = ((SqrlRelNode) added).getFields();
    System.out.println(fieldList.stream().map(e->e.getId()).collect(Collectors.toList()));
    System.out.println(added.getRowType().getFieldNames());
    System.out.println(added.getRowType());

    //If there is no alias & it is a single column then add it as a column to the parent table
    //todo this is a hack. Unnamed columns in calcite have the form of expr$
    System.out.println(fieldList);
    if (statement.getSql().contains("DISTINCT")) {
      System.out.println();
    }
    Optional<Field> unaliased = fieldList.stream()
        .filter(e->e.getName().getCanonical().contains("$")).findAny();
    if (unaliased.isPresent()) {
      NamePath namePath = statement.getNamePath().getPrefix()
          .orElseThrow(()->new RuntimeException("Unnamed queries cannot be assigned to root"));
      Table table = namespace.lookup(namePath)
          .orElseThrow(()->new RuntimeException("Could not find prefix table"));
      unaliased.get().setName(statement.getNamePath().getLast());
      table.addField(unaliased.get());

      System.out.println("SQL2: "+RelToSql.convertToSql(added));
      return;
    }

    Table destination = namespace.createTable(statement.getNamePath().getLast(), statement.getNamePath(), false);
    fieldList.forEach(destination::addField);

    //Assignment to root
    if (statement.getNamePath().getPrefix().isEmpty()) {
      List<Table> tables = new ArrayList<>();
      tables.add(destination);
      Dataset rootTable = new Dataset(Dataset.ROOT_NAMESPACE_NAME, tables);
      namespace.addDataset(rootTable);
      return;
    }

    Table source =
        namespace.lookup(statement.getNamePath().getPrefix().orElseThrow())
            .orElseThrow(()->new RuntimeException(String.format("Could not find table on field %s", statement.getNamePath())));
    Relationship fromField = new Relationship(statement.getNamePath().getLast(),
        source, destination, Type.JOIN, Multiplicity.MANY, null,null);
    source.addField(fromField);


    Relationship col = fromField;
    AliasGenerator gen = new AliasGenerator();

    SqlIdentifier left = new SqlIdentifier(col.getTable().getPath().toString(), SqlParserPos.ZERO);
    String la = gen.nextAlias();
    SqlIdentifier left_alias = new SqlIdentifier(la, SqlParserPos.ZERO);
    SqlIdentifier right = new SqlIdentifier(col.getToTable().getPath().toString(), SqlParserPos.ZERO);
    String ra = gen.nextAlias();
    SqlIdentifier right_alias = new SqlIdentifier(ra, SqlParserPos.ZERO);

    SqlIdentifier[] l = new SqlIdentifier[]{left, left_alias};
    SqlIdentifier[] r = new SqlIdentifier[]{right, right_alias};

    SqlJoin join = new SqlJoin(SqlParserPos.ZERO,
        new SqlBasicCall(OperatorTable.AS, l, SqlParserPos.ZERO),
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        SqlLiteral.createSymbol(JoinType.INNER, SqlParserPos.ZERO),
        new SqlBasicCall(OperatorTable.AS, r, SqlParserPos.ZERO),
        SqlLiteral.createSymbol(JoinConditionType.NONE, SqlParserPos.ZERO),
        null
    );
    SqlValidator validator = planner.getValidator(namespace);
    SqlSelect select = new SqlSelect(SqlParserPos.ZERO,
        new SqlNodeList(SqlParserPos.ZERO),
        new SqlNodeList(List.of(new SqlIdentifier(List.of(la), SqlParserPos.ZERO).plusStar()), SqlParserPos.ZERO),
        join, null, null, null, null, null, null, null, null
    );

    validator.validate(select);

    col.setSqlNode(join);

    Relationship toField = new Relationship(PARENT_RELATIONSHIP, destination, source, Type.PARENT, Multiplicity.ONE,
        fromField, null);
    fromField.setInverse(toField);
    destination.addField(toField);

    System.out.println("SQL: "+RelToSql.convertToSql(added));
  }

  public void process(CreateSubscription statement, Namespace namespace) {

  }
}
