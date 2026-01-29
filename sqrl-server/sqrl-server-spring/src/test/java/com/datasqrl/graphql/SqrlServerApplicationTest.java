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
package com.datasqrl.graphql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.graphql.config.ServerConfigProperties;
import org.junit.jupiter.api.Test;

class SqrlServerApplicationTest {

  @Test
  void givenDefaultConfig_whenGetExecutionMode_thenReturnsAsync() {
    var config = new ServerConfigProperties();
    assertThat(config.getExecutionMode()).isEqualTo(ServerConfigProperties.ExecutionMode.ASYNC);
  }

  @Test
  void givenDefaultConfig_whenGetServletConfig_thenReturnsDefaults() {
    var config = new ServerConfigProperties();
    var servletConfig = config.getServletConfig();

    assertThat(servletConfig).isNotNull();
    assertThat(servletConfig.getGraphqlEndpoint()).isEqualTo("/graphql");
    assertThat(servletConfig.getGraphiqlEndpoint()).isEqualTo("/graphiql/*");
    assertThat(servletConfig.getRestEndpoint()).isEqualTo("/rest");
    assertThat(servletConfig.getMcpEndpoint()).isEqualTo("/mcp");
  }

  @Test
  void givenVersionedEndpoint_whenGetEndpoint_thenReturnsVersionedPath() {
    var config = new ServerConfigProperties();
    var servletConfig = config.getServletConfig();

    assertThat(servletConfig.getGraphQLEndpoint("v1")).isEqualTo("/v1/graphql");
    assertThat(servletConfig.getRestEndpoint("v2")).isEqualTo("/v2/rest");
    assertThat(servletConfig.getMcpEndpoint("")).isEqualTo("/mcp");
  }
}
