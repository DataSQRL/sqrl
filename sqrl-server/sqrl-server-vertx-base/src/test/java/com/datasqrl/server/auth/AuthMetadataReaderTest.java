/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.server.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import graphql.GraphQLContext;
import graphql.Scalars;
import graphql.execution.ExecutionStepInfo;
import graphql.execution.MergedField;
import graphql.execution.ResultPath;
import graphql.language.Field;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingEnvironmentImpl;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import org.junit.jupiter.api.Test;

class AuthMetadataReaderTest {

  private final AuthMetadataReader authMetadataReader = new AuthMetadataReader();
  private final HttpServerResponse response = mock(HttpServerResponse.class, RETURNS_SELF);

  @Test
  void givenTopLevelClaim_whenRead_thenReturnsClaimValue() {
    var env = requestWithToken("{\"sub\": \"C1\"}");

    assertThat(authMetadataReader.read(env, "sub", true)).isEqualTo("C1");
  }

  @Test
  void givenNestedClaim_whenRead_thenReturnsNestedFieldValue() {
    var env = requestWithToken("{\"org\": {\"id\": \"ORG1\"}}");

    assertThat(authMetadataReader.read(env, "org.id", true)).isEqualTo("ORG1");
  }

  @Test
  void givenClaimThreeLevelsDeep_whenRead_thenReturnsDeepestFieldValue() {
    var env = requestWithToken("{\"org\": {\"location\": {\"zip\": \"10115\"}}}");

    assertThat(authMetadataReader.read(env, "org.location.zip", true)).isEqualTo("10115");
  }

  @Test
  void givenTopLevelClaimNameWithDots_whenRead_thenReturnsClaimValue() {
    var env = requestWithToken("{\"https://example.com/org_id\": \"ORG1\"}");

    assertThat(authMetadataReader.read(env, "https://example.com/org_id", true)).isEqualTo("ORG1");
  }

  @Test
  void givenNestedFieldOfClaimNameWithDots_whenRead_thenReturnsNestedFieldValue() {
    var env = requestWithToken("{\"https://example.com/org\": {\"id\": \"ORG1\"}}");

    assertThat(authMetadataReader.read(env, "https://example.com/org.id", true)).isEqualTo("ORG1");
  }

  @Test
  void givenTopLevelAndNestedClaimWithSameName_whenRead_thenReturnsTopLevelClaimValue() {
    var env = requestWithToken("{\"org.id\": \"TOP_LEVEL\", \"org\": {\"id\": \"NESTED\"}}");

    assertThat(authMetadataReader.read(env, "org.id", true)).isEqualTo("TOP_LEVEL");
  }

  @Test
  void givenLongestClaimNameWithoutPath_whenRead_thenDoesNotTryShorterClaimName() {
    var env = requestWithToken("{\"a.b\": \"x\", \"a\": {\"b\": {\"c\": 1}}}");

    assertThatThrownBy(() -> authMetadataReader.read(env, "a.b.c", true))
        .isInstanceOf(MissingRequiredClaimException.class);
  }

  @Test
  void givenNestedFieldNameWithDots_whenRead_thenReturnsNestedFieldValue() {
    var env =
        requestWithToken(
            "{\"resource_access\": {\"com.example.app\": {\"roles\": [\"admin\", \"user\"]}}}");

    assertThat(authMetadataReader.read(env, "resource_access.com.example.app.roles", true))
        .isEqualTo(new JsonArray().add("admin").add("user"));
  }

  @Test
  void givenNestedFieldWithDotsAndNestedPathWithSameName_whenRead_thenReturnsFieldWithDots() {
    var env =
        requestWithToken("{\"org\": {\"a.b\": \"FIELD_WITH_DOTS\", \"a\": {\"b\": \"NESTED\"}}}");

    assertThat(authMetadataReader.read(env, "org.a.b", true)).isEqualTo("FIELD_WITH_DOTS");
  }

  @Test
  void givenLongestNestedFieldNameWithoutPath_whenRead_thenDoesNotTryShorterFieldName() {
    var env = requestWithToken("{\"org\": {\"a.b\": \"x\", \"a\": {\"b\": {\"c\": 1}}}}");

    assertThatThrownBy(() -> authMetadataReader.read(env, "org.a.b.c", true))
        .isInstanceOf(MissingRequiredClaimException.class);
  }

  @Test
  void givenNestedArrayClaim_whenRead_thenReturnsWholeArray() {
    var env = requestWithToken("{\"realm_access\": {\"roles\": [\"admin\", \"user\"]}}");

    assertThat(authMetadataReader.read(env, "realm_access.roles", true))
        .isEqualTo(new JsonArray().add("admin").add("user"));
  }

  @Test
  void givenPathIntoArrayItem_whenRead_thenThrowsMissingRequiredClaim() {
    var env = requestWithToken("{\"realm_access\": {\"roles\": [\"admin\"]}}");

    assertThatThrownBy(() -> authMetadataReader.read(env, "realm_access.roles.0", true))
        .isInstanceOf(MissingRequiredClaimException.class);
  }

  @Test
  void givenPathIntoTextClaim_whenRead_thenThrowsMissingRequiredClaim() {
    var env = requestWithToken("{\"org\": \"ORG1\"}");

    assertThatThrownBy(() -> authMetadataReader.read(env, "org.id", true))
        .isInstanceOf(MissingRequiredClaimException.class);
  }

  @Test
  void givenClaimNameWithTrailingDot_whenRead_thenThrowsMissingRequiredClaim() {
    var env = requestWithToken("{\"org\": {\"id\": \"ORG1\"}}");

    assertThatThrownBy(() -> authMetadataReader.read(env, "org.", true))
        .isInstanceOf(MissingRequiredClaimException.class);
    verify(response).setStatusCode(403);
  }

  @Test
  void givenMissingRequiredNestedClaim_whenRead_thenRespondsForbidden() {
    var env = requestWithToken("{\"org\": {\"name\": \"Acme\"}}");

    assertThatThrownBy(() -> authMetadataReader.read(env, "org.id", true))
        .isInstanceOfSatisfying(
            MissingRequiredClaimException.class,
            exception -> assertThat(exception.getClaimName()).isEqualTo("org.id"));
    verify(response).setStatusCode(403);
  }

  @Test
  void givenMissingOptionalNestedClaim_whenRead_thenReturnsNull() {
    var env = requestWithToken("{\"sub\": \"C1\"}");

    assertThat(authMetadataReader.read(env, "org.id", false)).isNull();
    verify(response, never()).setStatusCode(403);
  }

  @Test
  void givenRequiredNestedClaimSetToNull_whenRead_thenThrowsMustNotBeNull() {
    var env = requestWithToken("{\"org\": {\"id\": null}}");

    assertThatThrownBy(() -> authMetadataReader.read(env, "org.id", true))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Claim 'org.id' must not be null");
    verify(response, never()).setStatusCode(403);
  }

  /**
   * Creates a request with an authenticated user. The user holds the claims the same way as the
   * Vert.x JWT provider: the full token in the attribute 'accessToken', which the attribute
   * 'rootClaim' names, and each claim in the principal.
   */
  private DataFetchingEnvironment requestWithToken(String tokenClaimsJson) {
    var tokenClaims = new JsonObject(tokenClaimsJson);
    var attributes =
        new JsonObject().put("accessToken", tokenClaims).put("rootClaim", "accessToken");
    var user = User.create(tokenClaims.copy(), attributes);

    var routingContext = mock(RoutingContext.class);
    when(routingContext.user()).thenReturn(user);
    when(routingContext.response()).thenReturn(response);

    return DataFetchingEnvironmentImpl.newDataFetchingEnvironment()
        .graphQLContext(
            GraphQLContext.newContext().of(RoutingContext.class, routingContext).build())
        .mergedField(MergedField.newMergedField(Field.newField("query").build()).build())
        .executionStepInfo(
            ExecutionStepInfo.newExecutionStepInfo()
                .type(Scalars.GraphQLString)
                .path(ResultPath.rootPath())
                .build())
        .build();
  }
}
