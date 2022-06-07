package ai.datasqrl.plan.local;

import static ai.datasqrl.parse.util.SqrlNodeUtil.hasOneUnnamedColumn;
import static ai.datasqrl.plan.util.FlinkSchemaUtil.requiresShredding;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.execute.flink.ingest.schema.FlinkTableConverter;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.CreateSubscription;
import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.JoinAssignment;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.QueryAssignment;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.physical.util.RelToSql;
import ai.datasqrl.plan.calcite.MultiphaseOptimizer;
import ai.datasqrl.plan.calcite.SqrlPrograms;
import ai.datasqrl.plan.local.operations.AddDatasetOp;
import ai.datasqrl.plan.local.operations.AddFieldOp;
import ai.datasqrl.plan.local.operations.AddNestedQueryOp;
import ai.datasqrl.plan.local.operations.AddQueryOp;
import ai.datasqrl.plan.local.operations.SchemaUpdateOp;
import ai.datasqrl.plan.local.shred.ShredPlanner;
import ai.datasqrl.plan.local.transpiler.StatementNormalizer;
import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinDeclarationNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.QuerySpecNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.TableNodeNorm;
import ai.datasqrl.plan.local.transpiler.toSql.ConvertContext;
import ai.datasqrl.plan.local.transpiler.toSql.SqlNodeConverter;
import ai.datasqrl.plan.local.transpiler.toSql.SqlNodeFormatter;
import ai.datasqrl.plan.util.FlinkRelDataTypeConverter;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.JoinType;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.factory.TableFactory;
import ai.datasqrl.server.ImportManager;
import ai.datasqrl.server.ImportManager.SourceTableImport;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;


@AllArgsConstructor
public class SchemaUpdatePlanner {

  private final ImportManager importManager;
  private final ErrorCollector errors;
  private final FlinkTableConverter tbConverter = new FlinkTableConverter();
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
      if (node.getNamePath().getLength() > 2) {
        throw new RuntimeException(
            String.format("Cannot import identifier: %s", node.getNamePath()));
      }

      SourceTableImport importSource = importManager
          .resolveTable(node.getNamePath().get(0), node.getNamePath().get(1), node.getAliasName(),
              errors);

      Pair<org.apache.flink.table.api.Schema, TypeInformation> tbl = tbConverter
          .tableSchemaConversion(importSource.getSourceSchema());
      org.apache.flink.table.api.Schema flinkSchema = tbl.getLeft();

      Name tableName = node.getAliasName().orElse(importSource.getTableName());

      List<Column> columns =
          FlinkRelDataTypeConverter.buildColumns(flinkSchema.getColumns());

      SourceTableStatistics sourceStats = importSource.getTable().getStatistics();

      ShredPlanner shredPlanner = new ShredPlanner();
      TableFactory tableFactory = new TableFactory();
      Table table = tableFactory.createSourceTable(tableName.toNamePath(), columns,
              sourceStats.getRelationStats(NamePath.ROOT));
      RelNode relNode = shredPlanner.plan(tableName, localPlanner.getCalcitePlanner().createRelBuilder(), tbl.getLeft(),
              table);

      table.setHead(relNode);

      if (requiresShredding(flinkSchema)) {
        shredPlanner.shred(tableName, flinkSchema, table, sourceStats,
                localPlanner.getCalcitePlanner().createRelBuilder());
      }

      return new AddDatasetOp(List.of(table));
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
      int nextVersion = table.getNextFieldVersion(name.getLast());

      //By convention, the last field is new the expression
      RelDataTypeField relField = relNode.getRowType().getFieldList().get(relNode.getRowType().getFieldList().size() - 1);
      Column column = new Column(name.getLast(), nextVersion, relField, List.of(), false,
          false, false, false);

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
      List<Column> columns = new ArrayList<>();
      List<Expression> select = specNorm.getSelect().getSelectItems().stream().map(e->e.getExpression()).collect(
          Collectors.toList());
      List<Expression> expressions = specNorm.getPrimaryKeys();

      //Internal columns
      List<Expression> addedPrimaryKeys = specNorm.getAddedPrimaryKeys();
      for (int i = 0; i < addedPrimaryKeys.size(); i++) {
        Expression expression = addedPrimaryKeys.get(i);
        Name name = specNorm.getFieldName(expression);
        Preconditions.checkNotNull(name);
        RelDataTypeField datatype = relNode.getRowType().getFieldList().get(
                specNorm.getParentPrimaryKeys().size() + i);
        Column column = new Column(name, 0, datatype, List.of(), true,
            expressions.contains(expression), false, false);
        columns.add(column);
      }

      for (int i = 0; i < select.size(); i++) {
        Expression s = select.get(i);
        Name name = specNorm.getFieldName(s);
        Preconditions.checkNotNull(name);
        RelDataTypeField datatype = relNode.getRowType().getFieldList().get(
                specNorm.getParentPrimaryKeys().size() + specNorm.getAddedPrimaryKeys().size() + i);
        Column column = new Column(name, 0, datatype, List.of(), false,
            expressions.contains(s), false, false);
        columns.add(column);
      }

      //TODO: infer table type from relNode
      Table.Type tblType = Table.Type.STREAM;

      //Preconditions.checkState(columns.stream().anyMatch(Column::isPrimaryKey), "No primary key was found");

      //Creates a table that is not bound to the schema
      TableFactory tableFactory = new TableFactory();
      Table table = tableFactory.createTable(namePath, tblType, columns, derivedRowCount);
      table.setHead(relNode);
      System.out.println(relNode.explain());

      if (namePath.getLength() == 1) {
        return new AddQueryOp(table);
      } else {
        Table parentTable = schema.walkTable(namePath.popLast());
        Name relationshipName = namePath.getLast();

        return new AddNestedQueryOp(parentTable, table, relationshipName);
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
