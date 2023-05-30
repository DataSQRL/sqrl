package com.datasqrl.graphql.inference;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.schema.Column;
import com.datasqrl.schema.SQRLTable;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SchemaInferenceTest {
  ModuleLoader loader;
  SqrlSchema schema;
  RelBuilder relBuilder;
  SQRLTable table;

  @BeforeEach
  public void before() {
    loader = mock(ModuleLoader.class);
    schema = mock(SqrlSchema.class);
    relBuilder = mock(RelBuilder.class);
    table = mock(SQRLTable.class);
  }

  @Test
  public void testViewsByFieldType() {
    when(schema.getRootTables()).thenReturn(List.of(table));
    when(table.getName()).thenReturn(Name.system("User"));

    Column field = mock(Column.class);
    when(table.getField(Name.system("id"))).thenReturn(Optional.of(field));

    /*
     * IMPORT User;
     */
    SchemaInference schemaInference = new SchemaInference(
        loader, "schema",
        "type Query { "
            + "  getUsers: [User] "
            + "  getUserById(id: String!): User "
            + "}"
            + "type User { id: String }",
        schema,
        relBuilder,
        null
    );

    InferredSchema inferredSchema = schemaInference.accept();
    assertNotNull(inferredSchema.getQuery());
  }

  @Test
  public void testViewsByCommonInterface() {
    when(schema.getRootTables()).thenReturn(List.of(table));
    when(table.getName()).thenReturn(Name.system("User"));

    Column field = mock(Column.class);
    when(table.getField(Name.system("id"))).thenReturn(Optional.of(field));
    Column username = mock(Column.class);
    when(table.getField(Name.system("username"))).thenReturn(Optional.of(username));

    /*
     * IMPORT User;
     */
    SchemaInference schemaInference = new SchemaInference(
        loader, "schema",
        "type Query { "
            + "  getUsers: [UserReduced] "
            + "  getUsersExtended: [UserExtended] "
            + "}"
            + "interface User { id: String }"
            + "type UserReduced implements User { username: String }"
            + "type UserExtended implements User { id: String, username: String }"
            + "",
        schema,
        relBuilder,
        null
    );

    InferredSchema inferredSchema = schemaInference.accept();
    assertNotNull(inferredSchema.getQuery());
  }
}