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
package com.datasqrl.graphql.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import org.junit.jupiter.api.Test;

class ServerConfigUtilTest {

  @Test
  void given_validVersionAndEndpoint_when_getVersionedEndpoint_then_returnsVersionedPath() {
    // given
    var version = "v1";
    var endpoint = "/graphql";

    // when
    var result = ServerConfigUtil.getVersionedEndpoint(version, endpoint);

    // then
    assertThat(result).isEqualTo("/v1/graphql");
  }

  @Test
  void given_nullEndpoint_when_getVersionedEndpoint_then_returnsNull() {
    // given
    var version = "v1";
    String endpoint = null;

    // when
    var result = ServerConfigUtil.getVersionedEndpoint(version, endpoint);

    // then
    assertThat(result).isNull();
  }

  @Test
  void given_emptyVersion_when_getVersionedEndpoint_then_returnsVersionedPath() {
    // given
    var version = "";
    var endpoint = "/graphql";

    // when
    var result = ServerConfigUtil.getVersionedEndpoint(version, endpoint);

    // then
    assertThat(result).isEqualTo("//graphql");
  }

  @Test
  void given_endpointWithLeadingSlash_when_getVersionedEndpoint_then_returnsVersionedPath() {
    // given
    var version = "v2";
    var endpoint = "/api/graphql";

    // when
    var result = ServerConfigUtil.getVersionedEndpoint(version, endpoint);

    // then
    assertThat(result).isEqualTo("/v2/api/graphql");
  }

  @Test
  void given_endpointWithoutLeadingSlash_when_getVersionedEndpoint_then_returnsVersionedPath() {
    // given
    var version = "v1";
    var endpoint = "graphql";

    // when
    var result = ServerConfigUtil.getVersionedEndpoint(version, endpoint);

    // then
    assertThat(result).isEqualTo("/v1graphql");
  }

  @Test
  void given_nullOptions_when_createVersionedGraphiQLHandlerOptions_then_returnsNull() {
    // given
    var version = "v1";
    GraphiQLHandlerOptions options = null;

    // when
    var result = ServerConfigUtil.createVersionedGraphiQLHandlerOptions(version, options);

    // then
    assertThat(result).isNull();
  }

  @Test
  void
      given_optionsWithGraphQLUri_when_createVersionedGraphiQLHandlerOptions_then_updatesGraphQLUri() {
    // given
    var version = "v1";
    var options = new GraphiQLHandlerOptions();
    options.setGraphQLUri("/graphql");

    // when
    var result = ServerConfigUtil.createVersionedGraphiQLHandlerOptions(version, options);

    // then
    assertThat(result).isNotNull();
    assertThat(result.getGraphQLUri()).isEqualTo("/v1/graphql");
  }

  @Test
  void
      given_optionsWithGraphQLWSUri_when_createVersionedGraphiQLHandlerOptions_then_updatesGraphQLWSUri() {
    // given
    var version = "v1";
    var options = new GraphiQLHandlerOptions();
    options.setGraphWSQLUri("/graphql-ws");

    // when
    var result = ServerConfigUtil.createVersionedGraphiQLHandlerOptions(version, options);

    // then
    assertThat(result).isNotNull();
    assertThat(result.getGraphQLWSUri()).isEqualTo("/v1/graphql-ws");
  }

  @Test
  void given_optionsWithBothUris_when_createVersionedGraphiQLHandlerOptions_then_updatesBothUris() {
    // given
    var version = "api";
    var options = new GraphiQLHandlerOptions();
    options.setGraphQLUri("/graphql");
    options.setGraphWSQLUri("/subscriptions");

    // when
    var result = ServerConfigUtil.createVersionedGraphiQLHandlerOptions(version, options);

    // then
    assertThat(result).isNotNull();
    assertThat(result.getGraphQLUri()).isEqualTo("/api/graphql");
    assertThat(result.getGraphQLWSUri()).isEqualTo("/api/subscriptions");
  }

  @Test
  void
      given_emptyVersion_when_createVersionedGraphiQLHandlerOptions_then_updatesWithEmptyVersion() {
    // given
    var version = "";
    var options = new GraphiQLHandlerOptions();
    options.setGraphQLUri("/graphql");
    options.setGraphWSQLUri("/graphql-ws");

    // when
    var result = ServerConfigUtil.createVersionedGraphiQLHandlerOptions(version, options);

    // then
    assertThat(result).isNotNull();
    assertThat(result.getGraphQLUri()).isEqualTo("//graphql");
    assertThat(result.getGraphQLWSUri()).isEqualTo("//graphql-ws");
  }

  @Test
  void
      given_complexEndpoints_when_createVersionedGraphiQLHandlerOptions_then_handlesComplexPaths() {
    // given
    var version = "v2.1";
    var options = new GraphiQLHandlerOptions();
    options.setGraphQLUri("/api/v1/graphql");
    options.setGraphWSQLUri("/api/v1/subscriptions");

    // when
    var result = ServerConfigUtil.createVersionedGraphiQLHandlerOptions(version, options);

    // then
    assertThat(result).isNotNull();
    assertThat(result.getGraphQLUri()).isEqualTo("/v2.1/api/v1/graphql");
    assertThat(result.getGraphQLWSUri()).isEqualTo("/v2.1/api/v1/subscriptions");
  }

  @Test
  void
      given_originalOptions_when_createVersionedGraphiQLHandlerOptions_then_returnsNewInstanceWithoutModifyingOriginal() {
    // given
    var version = "v1";
    var options = new GraphiQLHandlerOptions();
    options.setGraphQLUri("/graphql");
    options.setGraphWSQLUri("/graphql-ws");
    var originalGraphQLUri = options.getGraphQLUri();
    var originalGraphQLWSUri = options.getGraphQLWSUri();

    // when
    var result = ServerConfigUtil.createVersionedGraphiQLHandlerOptions(version, options);

    // then
    assertThat(result).isNotSameAs(options);
    assertThat(result.getGraphQLUri()).isEqualTo("/v1/graphql");
    assertThat(result.getGraphQLWSUri()).isEqualTo("/v1/graphql-ws");

    // Verify original options remain unchanged
    assertThat(options.getGraphQLUri()).isEqualTo(originalGraphQLUri);
    assertThat(options.getGraphQLWSUri()).isEqualTo(originalGraphQLWSUri);
  }
}
