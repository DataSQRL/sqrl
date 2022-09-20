package ai.datasqrl.graphql2.generate;

import ai.datasqrl.graphql2.generate.SchemaBuilder.ObjectTypeBuilder;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.SQRLTable;
import graphql.schema.GraphQLSchema;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 * Creates a default graphql schema based on the SQRL schema
 */
public class SchemaGenerator {
  private final Env env;
  SchemaBuilder schemaBuilder = new SchemaBuilder();

  public SchemaGenerator(Env env) {
    this.env = env;
  }

  public static GraphQLSchema generate(Env env) {
    SchemaGenerator schemaGenerator = new SchemaGenerator(env);
    schemaGenerator.createTypes();
    schemaGenerator.generateRootQueries();
    return schemaGenerator.schemaBuilder.build();
  }

  private void createTypes() {
    for (SQRLTable table : getAllTables()) {
      ObjectTypeBuilder builder = schemaBuilder.createObjectType(table);
      for (Field field : table.getFields().getAccessibleFields()) {
        switch (field.getKind()) {
          case COLUMN:
            Column c = (Column) field;
            builder.createScalarField(c.getName(), c.getType());
            break;
          case RELATIONSHIP:
            Relationship r = (Relationship) field;
            builder.createRelationshipField(r.getName(), r.getToTable(), r.getMultiplicity());
            break;
          case TABLE_FUNCTION:
            break;
        }
      }
    }
  }

  private void generateRootQueries() {
    ObjectTypeBuilder builder = schemaBuilder.getQuery();
    for (SQRLTable table : getRootTables()) {
      builder.createRelationshipField(table.getName(), table, Multiplicity.MANY);
    }
  }

  private List<SQRLTable> getRootTables() {
    return env.getSqrlSchema().getTableNames().stream()
        .map(name -> (SQRLTable) env.getSqrlSchema().getTable(name, false).getTable())
        .collect(Collectors.toList());
  }

  private List<SQRLTable> getAllTables() {
    Set<SQRLTable> tables = new HashSet<>(getRootTables());
    Stack<SQRLTable> iter = new Stack<>();
    iter.addAll(getRootTables());

    while (!iter.isEmpty()) {
      SQRLTable table = iter.pop();
      List<SQRLTable> relationships = table.getFields().getAccessibleFields().stream()
          .filter(f-> f instanceof Relationship)
          .map(f->((Relationship)f).getToTable())
          .collect(Collectors.toList());

      for (SQRLTable rel : relationships) {
        if (!tables.contains(rel)) {
          iter.add(rel);
          tables.add(rel);
        }
      }
    }

    return new ArrayList<>(tables);
  }
}
