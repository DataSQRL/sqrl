package ai.datasqrl.plan.local;

import static ai.datasqrl.parse.util.SqrlNodeUtil.hasOneUnnamedColumn;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.parse.tree.*;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.local.operations.*;
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
import ai.datasqrl.schema.type.basic.DateTimeType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.tuple.Pair;


@AllArgsConstructor
public class SchemaUpdatePlanner {

  private final ImportManager importManager;
  private final BundleTableFactory tableFactory;
  private final SchemaAdjustmentSettings schemaSettings;
  private final ErrorCollector errors;

  public Optional<SchemaUpdateOp> plan(Schema schema, Node node) {
    return Optional.ofNullable(node.accept(new Visitor(schema), null));
  }

  private static AddColumnOp planExpression(Node node, NamePath name, Schema schema) {
    StatementNormalizer normalizer = new StatementNormalizer(schema);
    Node relationNorm = normalizer.normalize(node);
//      System.out.println("Converted: " + NodeFormatter.accept(relationNorm));
    SqlNodeConverter converter = new SqlNodeConverter();
    SqlNode sqlNode = relationNorm.accept(converter, new ConvertContext());
    System.out.println("Sql: " + SqlNodeFormatter.toString(sqlNode));

    Table table = schema.walkTable(name.popLast());
    Name columnName = name.getLast();
    int nextVersion = table.getNextColumnVersion(columnName);

    Column column = new Column(columnName, nextVersion, table.getFields().size(),
            false, false, true);

    return new AddColumnOp(table, relationNorm, column);
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
          Pair<Table, Map<Table,SourceTableImportOp.RowType>> resolvedImport = tableFactory.importTable(importSource, nameAlias);
          Table table = resolvedImport.getKey();
          Map<Table,SourceTableImportOp.RowType> tableTypes = resolvedImport.getValue();
          SourceTableImportOp op = new SourceTableImportOp(table, tableTypes, importSource);
          ops.add(op);
          //Add timestamp if explicitly defined
          Column timestampColumn;
          if (node.getTimestamp().isPresent()) {
            SingleColumn timeColDef = node.getTimestamp().get();
            Name timeColName = timeColDef.getAlias().map(n -> n.getNamePath().getLast()).orElse(ReservedName.TIMESTAMP);
            if (!timeColDef.getAlias().isPresent() && timeColDef.getExpression() instanceof Identifier) {
              //Check special case that colDef is just a simple reference to an existing column, in which
              //case we add it directly to the table without creating a new column.
              Identifier timestampId = (Identifier) timeColDef.getExpression();
              Optional<Field> column = table.walkField(timestampId.getNamePath());
              if (!column.isPresent() || !(column.get() instanceof Column) || timestampId.getNamePath().getLength()!=1) {
                throw new IllegalArgumentException("Invalid column identifier [" + timestampId.getNamePath() + "] for table: " + table);
              }
              Column timestampColum = (Column)column.get();
              SourceTableImportOp.ColumnType timestampType = tableTypes.get(table).get(timestampColum.getIndex());
              Preconditions.checkArgument(timestampType.isNotnull() && timestampType.getType() instanceof DateTimeType,
                      "Timestamp column has invalid type: " + timestampType);
              table.getTimestamp().update(timestampColum, TableTimestamp.Status.DEFINED);
            } else {
              //Create a new timestamp column on table
              SchemaBuilder schema = new SchemaBuilder();
              schema.apply(new SourceTableImportOp(table, tableTypes, importSource));
              NamePath colPath = NamePath.of(table.getName(),timeColName);
              ExpressionAssignment timeColAssign = new ExpressionAssignment(Optional.empty(),colPath,timeColDef.getExpression(),"",List.of());
              AddColumnOp timeCol = planExpression(timeColAssign,colPath,schema.peek());
              ops.add(timeCol.asTimestamp());
            }
          }
        } else {
          throw new UnsupportedOperationException("Script imports are not yet supported");
        }
      }
      return new MultipleUpdateOp(ops);
    }

    @Override
    public SchemaUpdateOp visitExpressionAssignment(ExpressionAssignment node, Object context) {
      return planExpression(node, node.getNamePath(), schema);
    }


    @Override
    public SchemaUpdateOp visitQueryAssignment(QueryAssignment node, Object context) {
      if (hasOneUnnamedColumn(node.getQuery())) {
        return planExpression(node, node.getNamePath(), schema);
      }

      StatementNormalizer normalizer = new StatementNormalizer(schema);
      Node relationNorm = normalizer.normalize(node);
      return query(relationNorm, node.getNamePath());
    }

    public SchemaUpdateOp query(Node relationNorm, NamePath namePath) {
      SqlNodeConverter converter = new SqlNodeConverter();
      SqlNode sqlNode = relationNorm.accept(converter, new ConvertContext());
      System.out.println("Sql: " + SqlNodeFormatter.toString(sqlNode));

      double derivedRowCount = 1; //TODO: derive from optimizer

      QuerySpecNorm specNorm = ((QuerySpecNorm)relationNorm);
      final BundleTableFactory.TableBuilder builder = tableFactory.build(namePath);

      specNorm.getAllColumns().stream().forEach(expr -> {
        Name name = specNorm.getFieldName(expr);
        builder.addColumn(name, specNorm.isPrimaryKey(expr), specNorm.isParentPrimaryKey(expr), specNorm.isVisible(expr));
      });
      //TODO: infer table type from relNode
      Table.Type tblType = Table.Type.STREAM;

      //Creates a table that is not bound to the schema TODO: determine timestamp
      Table table = builder.createTable(tblType, TableStatistic.of(derivedRowCount));

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
      StatementNormalizer normalizer = new StatementNormalizer(schema);
      Node relationNorm = normalizer.normalize(node);
      return query(relationNorm, node.getNamePath());
    }

    @Override
    public SchemaUpdateOp visitDistinctAssignment(DistinctAssignment node, Object context) {
      StatementNormalizer normalizer = new StatementNormalizer(schema);
      Node relationNorm = normalizer.normalize(node);
      return query(relationNorm, node.getNamePath());
    }

    @Override
    public SchemaUpdateOp visitJoinAssignment(JoinAssignment node, Object context) {
      StatementNormalizer normalizer = new StatementNormalizer(schema);
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
