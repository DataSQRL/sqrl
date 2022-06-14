package ai.datasqrl.plan.local;

import static ai.datasqrl.parse.util.SqrlNodeUtil.hasOneUnnamedColumn;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.parse.tree.*;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.physical.util.RelToSql;
import ai.datasqrl.plan.calcite.MultiphaseOptimizer;
import ai.datasqrl.plan.calcite.SqrlPrograms;
import ai.datasqrl.plan.local.operations.AddImportedTablesOp;
import ai.datasqrl.plan.local.operations.AddFieldOp;
import ai.datasqrl.plan.local.operations.AddNestedTableOp;
import ai.datasqrl.plan.local.operations.AddTableOp;
import ai.datasqrl.plan.local.operations.SchemaUpdateOp;
import ai.datasqrl.plan.local.transpiler.StatementNormalizer;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedColumn;
import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinDeclarationNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.QuerySpecNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.TableNodeNorm;
import ai.datasqrl.plan.local.transpiler.toSql.ConvertContext;
import ai.datasqrl.plan.local.transpiler.toSql.SqlNodeConverter;
import ai.datasqrl.plan.local.transpiler.toSql.SqlNodeFormatter;
import ai.datasqrl.schema.*;
import ai.datasqrl.schema.Relationship.JoinType;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;


@AllArgsConstructor
public class SchemaUpdatePlanner {

  private final ImportManager importManager;
  private final BundleTableFactory tableFactory;
  private final SchemaAdjustmentSettings schemaSettings;
  private final ErrorCollector errors;
  private final LocalPlanner localPlanner;

  public Optional<SchemaUpdateOp> plan(Schema schema, Node node) {
    return Optional.ofNullable(node.accept(new Visitor(schema), null));
  }

  private class Visitor extends AstVisitor<SchemaUpdateOp, Object> {

    private final Schema schema;

    public Visitor(Schema schema) {
      this.schema = schema;
    }

    @Override
    public SchemaUpdateOp visitImportDefinition(ImportDefinition node, Object context) {
      if (node.getNamePath().getLength() != 2) {
        throw new RuntimeException(
            String.format("Invalid import identifier: %s", node.getNamePath()));
      }

      //Check if this imports all or a single table
      Name sourceDataset = node.getNamePath().get(0);
      Name sourceTable = node.getNamePath().get(1);

      List<ImportManager.TableImport> importTables;
      Optional<Name> nameAlias;

      if (sourceTable.equals(ReservedName.ALL)) { //import all tables from dataset
        importTables = importManager.importAllTables(sourceDataset, schemaSettings, errors);
        nameAlias = Optional.empty();
      } else { //importing a single table
        importTables = List.of(importManager
                .importTable(sourceDataset, sourceTable, schemaSettings, errors));
        nameAlias = node.getAliasName();
      }

      List<Table> resultTables = new ArrayList<>();
      for (ImportManager.TableImport tblImport : importTables) {
        if (tblImport.isSource()) {
          SourceTableImport importSource = (SourceTableImport)tblImport;
          resultTables.add(tableFactory.importTable(localPlanner.getCalcitePlanner(), importSource, nameAlias));
        } else {
          throw new UnsupportedOperationException("Script imports are not yet supported");
        }
      }
      return new AddImportedTablesOp(resultTables);
    }

    @Override
    public SchemaUpdateOp visitExpressionAssignment(ExpressionAssignment node, Object context) {
      return planExpression(node, node.getNamePath());
    }

    protected SchemaUpdateOp planExpression(Node node, NamePath name) {
      StatementNormalizer normalizer = new StatementNormalizer(importManager, schema);
      Node relationNorm = normalizer.normalize(node);
//      System.out.println("Converted: " + NodeFormatter.accept(relationNorm));
      SqlNodeConverter converter = new SqlNodeConverter();
      SqlNode sqlNode = relationNorm.accept(converter, new ConvertContext());
      System.out.println("Sql: " + SqlNodeFormatter.toString(sqlNode));

      RelNode relNode = localPlanner.plan(sqlNode);
      System.out.println("RelNode:\n" + relNode.explain());
      System.out.println(RelToSql.convertToSql(relNode));

      MultiphaseOptimizer optimizer = new MultiphaseOptimizer();
      RelNode optimized = optimizer.optimize(relNode, SqrlPrograms.testProgram);

      System.out.println(
          RelOptUtil.dumpPlan("[Physical plan]", optimized, SqlExplainFormat.TEXT,
              SqlExplainLevel.ALL_ATTRIBUTES));

      Table table = schema.walkTable(name.popLast());
      Name columnName = name.getLast();
      int nextVersion = table.getNextFieldVersion(columnName);

      //By convention, the last field is new the expression
      int index = relNode.getRowType().getFieldList().size() - 1;
      RelDataTypeField relField = relNode.getRowType().getFieldList().get(index);
      Column column = new Column(columnName, nextVersion, index, relField.getType(),
              false, false, List.of(), false);

      return new AddFieldOp(table, column, Optional.of(relNode));
    }

    @Override
    public SchemaUpdateOp visitQueryAssignment(QueryAssignment node, Object context) {
      if (hasOneUnnamedColumn(node.getQuery())) {
        return planExpression(node, node.getNamePath());
      }

      StatementNormalizer normalizer = new StatementNormalizer(importManager, schema);
      Node relationNorm = normalizer.normalize(node);
      return query(relationNorm, node.getNamePath());
    }

    public SchemaUpdateOp query(Node relationNorm, NamePath namePath) {
//      System.out.println("Converted: " + NodeFormatter.accept(relationNorm));
      SqlNodeConverter converter = new SqlNodeConverter();
      SqlNode sqlNode = relationNorm.accept(converter, new ConvertContext());
      System.out.println("Sql: " + SqlNodeFormatter.toString(sqlNode));

      RelNode relNode = localPlanner.plan(sqlNode);

      MultiphaseOptimizer optimizer = new MultiphaseOptimizer();
      RelNode optimized = optimizer.optimize(relNode, SqrlPrograms.testProgram);

      double derivedRowCount = 1; //TODO: derive from optimizer
      // double derivedRowCount = optimized.estimateRowCount(??);

      System.out.println(
          RelOptUtil.dumpPlan("[Physical plan]", optimized, SqlExplainFormat.TEXT,
              SqlExplainLevel.ALL_ATTRIBUTES));
      //To column list
      QuerySpecNorm specNorm = ((QuerySpecNorm)relationNorm);
      List<Expression> select = specNorm.getSelect().getSelectItems().stream().map(e->e.getExpression()).collect(
          Collectors.toList());
      List<Expression> primaryKeys = specNorm.getPrimaryKeys();
      List<Expression> addedPrimaryKeys = specNorm.getAddedPrimaryKeys();
      List<? extends Expression> parentPrimaryKeys = specNorm.getParentPrimaryKeys();

      BundleTableFactory.TableBuilder builder = tableFactory.build(namePath);

      for (RelDataTypeField field : relNode.getRowType().getFieldList()) {
        int index = field.getIndex();
        Expression expression;
        int i = index;
        if (i < parentPrimaryKeys.size()) {
          expression = parentPrimaryKeys.get(i);
        }
        i = i - parentPrimaryKeys.size();
        if (i < addedPrimaryKeys.size()) {
          expression = addedPrimaryKeys.get(i);
        }
        i = i - addedPrimaryKeys.size();
        expression = select.get(i);
        Name name = specNorm.getFieldName(expression);
        Preconditions.checkNotNull(name);
        builder.addColumn(name,field.getType(),true, true, true, true);
      }

      //TODO: infer table type from relNode
      Table.Type tblType = Table.Type.STREAM;

      //Preconditions.checkState(columns.stream().anyMatch(Column::isPrimaryKey), "No primary key was found");

      //Creates a table that is not bound to the schema TODO: determine timestamp
      Table table = builder.createTable(tblType, null, relNode, TableStatistic.of(derivedRowCount));
      System.out.println(relNode.explain());

      if (namePath.getLength() == 1) {
        return new AddTableOp(table);
      } else {
        Table parentTable = schema.walkTable(namePath.popLast());
        Name relationshipName = namePath.getLast();
        Relationship.Multiplicity multiplicity = Multiplicity.MANY;
        if (specNorm.getLimit().flatMap(Limit::getIntValue).orElse(2) == 1) {
          multiplicity = Multiplicity.ONE;
        }

        return new AddNestedTableOp(parentTable, table, relationshipName, multiplicity);
      }

    }

    @Override
    public SchemaUpdateOp visitCreateSubscription(CreateSubscription node, Object context) {
      StatementNormalizer normalizer = new StatementNormalizer(importManager, schema);
      Node relationNorm = normalizer.normalize(node);
      return query(relationNorm, node.getNamePath());
    }

    @Override
    public SchemaUpdateOp visitDistinctAssignment(DistinctAssignment node, Object context) {
      StatementNormalizer normalizer = new StatementNormalizer(importManager, schema);
      Node relationNorm = normalizer.normalize(node);
      return query(relationNorm, node.getNamePath());
    }

    @Override
    public SchemaUpdateOp visitJoinAssignment(JoinAssignment node, Object context) {
      StatementNormalizer normalizer = new StatementNormalizer(importManager, schema);
      Node normalized = normalizer.normalize(node);
      JoinDeclarationNorm join = (JoinDeclarationNorm) normalized;
      Table parentTable = schema.walkTable(node.getNamePath().popLast());
      Multiplicity multiplicity = node.getJoinDeclaration().getLimit()
          .map(l-> l.getIntValue().filter(i -> i == 1).map(i -> Multiplicity.ONE)
              .orElse(Multiplicity.MANY))
          .orElse(Multiplicity.MANY);
      Relationship relationship = new Relationship(node.getNamePath().getLast(),
          parentTable,
          ((TableNodeNorm) join.getRelation().getRightmost())
              .getRef().getTable(),
          JoinType.JOIN,
          multiplicity,
          join.getRelation(),
          join.getOrderBy(),
          node.getJoinDeclaration().getLimit()
      );

      SqlNodeConverter converter = new SqlNodeConverter();
      SqlNode sqlNode = join.getRelation().accept(converter, new ConvertContext());
      //TODO: Do a validation pass on join declarations (compose new query manually w/ selects)
//      SqlValidator validator = localPlanner.getCalcitePlanner().createValidator();
//      validator.validate(sqlNode);

      return new AddFieldOp(
          parentTable,
          relationship,
          Optional.empty()
      );
    }
  }
}
