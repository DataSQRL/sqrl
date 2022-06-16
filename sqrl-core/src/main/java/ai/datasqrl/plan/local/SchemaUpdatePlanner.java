package ai.datasqrl.plan.local;

import static ai.datasqrl.parse.util.SqrlNodeUtil.hasOneUnnamedColumn;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.parse.tree.*;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.local.operations.AddJoinDeclarationOp;
import ai.datasqrl.plan.local.operations.MultipleUpdateOp;
import ai.datasqrl.plan.local.operations.SourceTableImportOp;
import ai.datasqrl.plan.local.operations.AddColumnOp;
import ai.datasqrl.plan.local.operations.AddNestedTableOp;
import ai.datasqrl.plan.local.operations.AddRootTableOp;
import ai.datasqrl.plan.local.operations.SchemaUpdateOp;
import ai.datasqrl.plan.local.transpiler.StatementNormalizer;
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
import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlNode;


@AllArgsConstructor
public class SchemaUpdatePlanner {

  private final ImportManager importManager;
  private final BundleTableFactory tableFactory;
  private final SchemaAdjustmentSettings schemaSettings;
  private final ErrorCollector errors;

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

      List<SchemaUpdateOp> ops = new ArrayList<>();

      for (ImportManager.TableImport tblImport : importTables) {
        if (tblImport.isSource()) {
          SourceTableImport importSource = (SourceTableImport)tblImport;
          Table table = tableFactory.importTable(importSource, nameAlias);
          SourceTableImportOp op = new SourceTableImportOp(table, importSource);
          ops.add(op);
        } else {
          throw new UnsupportedOperationException("Script imports are not yet supported");
        }
      }
      return new MultipleUpdateOp(ops);
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

//      RelNode relNode = localPlanner.plan(sqlNode);
//      System.out.println("RelNode:\n" + relNode.explain());
//      System.out.println(RelToSql.convertToSql(relNode));

//      MultiphaseOptimizer optimizer = new MultiphaseOptimizer();
//      RelNode optimized = optimizer.optimize(relNode, SqrlPrograms.testProgram);
//
//      System.out.println(
//          RelOptUtil.dumpPlan("[Physical plan]", optimized, SqlExplainFormat.TEXT,
//              SqlExplainLevel.ALL_ATTRIBUTES));

      Table table = schema.walkTable(name.popLast());
      Name columnName = name.getLast();
      int nextVersion = table.getNextColumnVersion(columnName);

      //By convention, the last field is new the expression
//      int index = relNode.getRowType().getFieldList().size() - 1;
//      RelDataTypeField relField = relNode.getRowType().getFieldList().get(index);

      Column column = new Column(columnName, nextVersion, 0, null,
              false, false, List.of(), false);

      return new AddColumnOp(table, relationNorm, column);
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

//      RelNode relNode = localPlanner.plan(sqlNode);
//      MultiphaseOptimizer optimizer = new MultiphaseOptimizer();
//      RelNode optimized = optimizer.optimize(relNode, SqrlPrograms.testProgram);

      double derivedRowCount = 1; //TODO: derive from optimizer
      // double derivedRowCount = optimized.estimateRowCount(??);

//      System.out.println(
//          RelOptUtil.dumpPlan("[Physical plan]", optimized, SqlExplainFormat.TEXT,
//              SqlExplainLevel.ALL_ATTRIBUTES));
      //To column list
      QuerySpecNorm specNorm = ((QuerySpecNorm)relationNorm);
      List<Expression> select = specNorm.getSelect().getSelectItems().stream().map(e->e.getExpression()).collect(
          Collectors.toList());

      final BundleTableFactory.TableBuilder builder = tableFactory.build(namePath);

      Streams.concat(
          specNorm.getAddedPrimaryKeys().stream(),
          specNorm.getParentPrimaryKeys().stream()
      ).forEach(expr -> {
        Name name = specNorm.getFieldName(expr);
        builder.addColumn(name, null,true, true, true, true);
      });

      specNorm.getPrimaryKeys().stream().forEach(expr -> {
        Name name = specNorm.getFieldName(expr);
        builder.addColumn(name, null,true, false, true, false);
      });

      select.stream()
          .forEach(expr -> {
            Name name = specNorm.getFieldName(expr);
            builder.addColumn(name, null,false, false, true, false);
          });


//      List<Expression> primaryKeys = specNorm.getPrimaryKeys();
//      List<Expression> addedPrimaryKeys = specNorm.getAddedPrimaryKeys();
//      List<? extends Expression> parentPrimaryKeys = specNorm.getParentPrimaryKeys();

//      for (RelDataTypeField field : relNode.getRowType().getFieldList()) {
//        int index = field.getIndex();
//        Expression expression;
//        int i = index;
//        if (i < parentPrimaryKeys.size()) {
//          expression = parentPrimaryKeys.get(i);
//        }
//        i = i - parentPrimaryKeys.size();
//        if (i < addedPrimaryKeys.size()) {
//          expression = addedPrimaryKeys.get(i);
//        }
//        i = i - addedPrimaryKeys.size();
//        expression = select.get(i);
//        Name name = specNorm.getFieldName(expression);
//        Preconditions.checkNotNull(name);
//        builder.addColumn(name, field.getType(),true, true, true, true);
//      }

      //TODO: infer table type from relNode
      Table.Type tblType = Table.Type.STREAM;

//      Preconditions.checkState(columns.stream().anyMatch(Column::isPrimaryKey), "No primary key was found");

      //Creates a table that is not bound to the schema TODO: determine timestamp
      Table table = builder.createTable(tblType, null, null, TableStatistic.of(derivedRowCount));
//      System.out.println(relNode.explain());

      if (namePath.getLength() == 1) {
        return new AddRootTableOp(table, relationNorm);
      } else {
        Table parentTable = schema.walkTable(namePath.popLast());
        Name relationshipName = namePath.getLast();
        Relationship.Multiplicity multiplicity = Multiplicity.MANY;
        if (specNorm.getLimit().flatMap(Limit::getIntValue).orElse(2) == 1) {
          multiplicity = Multiplicity.ONE;
        }

        List<SchemaUpdateOp> ops = new ArrayList<>();

        Optional<Relationship> parentRel = tableFactory.createParentRelationship(table, parentTable);
        Optional<SchemaUpdateOp> parentFieldOp = parentRel.map(rel -> new AddJoinDeclarationOp(table, rel.getRelation(), rel));
        parentFieldOp.map(ops::add);

        Relationship childRel = tableFactory.createChildRelationship(relationshipName, table, parentTable, multiplicity);
        SchemaUpdateOp childFieldOp = new AddJoinDeclarationOp(parentTable, childRel.getRelation(), childRel);
        ops.add(childFieldOp);

        AddNestedTableOp tableOp = new AddNestedTableOp(table, relationNorm);
        ops.add(tableOp);

        return new MultipleUpdateOp(ops);
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

      return new AddJoinDeclarationOp(
          parentTable,
          relationship.getRelation(),
          relationship
      );
    }
  }
}
