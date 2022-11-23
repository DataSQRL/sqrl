package ai.datasqrl.graphql.generate;

import ai.datasqrl.graphql.generate.SchemaBuilder.ObjectTypeBuilder;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.SQRLTable;
import graphql.schema.GraphQLSchema;
import org.apache.calcite.jdbc.SqrlCalciteSchema;

/**
 * Creates a default graphql schema based on the SQRL schema
 */
public class SchemaGenerator {
  private final SqrlCalciteSchema schema;
  SchemaBuilder schemaBuilder = new SchemaBuilder();

  public SchemaGenerator(SqrlCalciteSchema schema) {
    this.schema = schema;
  }

  public static GraphQLSchema generate(SqrlCalciteSchema schema) {
    SchemaGenerator schemaGenerator = new SchemaGenerator(schema);
    schemaGenerator.createTypes();
    schemaGenerator.generateRootQueries();
    return schemaGenerator.schemaBuilder.build();
  }

  private void createTypes() {
    for (SQRLTable table : schema.getAllTables()) {
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
    for (SQRLTable table : schema.getRootTables()) {
      builder.createRelationshipField(table.getName(), table, Multiplicity.MANY);
    }
  }
}
