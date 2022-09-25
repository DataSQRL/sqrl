package ai.datasqrl.graphql.generate;

import ai.datasqrl.graphql.generate.SchemaBuilder.ObjectTypeBuilder;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.SQRLTable;
import graphql.schema.GraphQLSchema;

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
    for (SQRLTable table : env.getSqrlSchema().getAllTables()) {
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
    for (SQRLTable table : env.getSqrlSchema().getRootTables()) {
      builder.createRelationshipField(table.getName(), table, Multiplicity.MANY);
    }
  }
}
