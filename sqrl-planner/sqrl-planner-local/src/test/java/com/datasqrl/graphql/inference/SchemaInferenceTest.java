package com.datasqrl.graphql.inference;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.local.analyze.MockAPIConnectorManager;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.schema.Column;
import com.datasqrl.schema.SQRLTable;

import java.util.*;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apiguardian.api.API;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

class SchemaInferenceTest {
  ModuleLoader loader;
  SqrlSchema schema;
  RelBuilder relBuilder;
  SQRLTable table;

  @BeforeEach
  public void before() {
    MockitoAnnotations.initMocks(this);
    loader = mock(ModuleLoader.class);
    schema = mock(SqrlSchema.class);
    relBuilder = mock(RelBuilder.class);
    table = mock(SQRLTable.class);

    when(schema.getFunction(anyString()))
        .thenReturn(new ArrayList<>());
  }

  @Test
  public void testViewsByFieldType() {
    when(schema.getRootTables()).thenReturn(List.of(table));
    when(table.getName()).thenReturn(Name.system("User"));

    Column field = mock(Column.class);
    when(field.getType()).thenReturn(new JavaTypeFactoryImpl()
        .createSqlType(SqlTypeName.VARCHAR));
    when(table.getField(Name.system("id"))).thenReturn(Optional.of(field));

    /*
     * IMPORT User;
     */
    APISource gql = APISource.of("type Query { "
        + "  getUsers: [User] "
        + "  getUserById(id: String!): User "
        + "}"
        + "type User { id: String }");

    SchemaInference schemaInference = new SchemaInference(
        "schema", loader, gql,
        schema,
        relBuilder,
        null, new MockAPIConnectorManager()
    );

    InferredSchema inferredSchema = schemaInference.accept();
    assertNotNull(inferredSchema.getQuery());
  }

  @Test
  public void testViewsByCommonInterface() {
    when(schema.getRootTables()).thenReturn(List.of(table));
    when(table.getName()).thenReturn(Name.system("User"));

    Column field = mock(Column.class);
    when(field.getType()).thenReturn(new JavaTypeFactoryImpl()
        .createSqlType(SqlTypeName.VARCHAR));
    when(table.getField(Name.system("id"))).thenReturn(Optional.of(field));
    Column username = mock(Column.class);
    when(username.getType()).thenReturn(new JavaTypeFactoryImpl()
        .createSqlType(SqlTypeName.VARCHAR));
    when(table.getField(Name.system("username"))).thenReturn(Optional.of(username));


    APISource gql = APISource.of("type Query { "
        + "  getUsers: [UserReduced] "
        + "  getUsersExtended: [UserExtended] "
        + "}"
        + "interface User { id: String }"
        + "type UserReduced implements User { username: String }"
        + "type UserExtended implements User { id: String, username: String }");
    /*
     * IMPORT User;
     */
    SchemaInference schemaInference = new SchemaInference(
        "schema", loader, gql,
        schema,
        relBuilder,
        null, new MockAPIConnectorManager()
    );

    InferredSchema inferredSchema = schemaInference.accept();
    assertNotNull(inferredSchema.getQuery());
  }
}