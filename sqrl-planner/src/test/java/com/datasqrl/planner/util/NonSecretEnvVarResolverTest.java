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
package com.datasqrl.planner.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class NonSecretEnvVarResolverTest {

  private NonSecretEnvVarResolver resolver;

  @ParameterizedTest
  @MethodSource("secretEnvTemplates")
  void givenSecretEnvTemplate_whenResolve_thenConvertToStandardEnvTemplate(
      String command, String expected) {
    resolver = NonSecretEnvVarResolver.builder().envVars(Map.of("HOST", "db.example.com")).build();

    var result = resolver.resolve(command);

    assertThat(result).isEqualTo(expected);
  }

  private static Stream<Arguments> secretEnvTemplates() {
    return Stream.of(
        arguments("${{SECRET}}", "${SECRET}"),
        arguments("Token=${{SECRET}}", "Token=${SECRET}"),
        arguments(
            "Host=${HOST}, password=${{DB_PASSWORD}}",
            "Host=db.example.com, password=${DB_PASSWORD}"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "${{TOKEN:-default}}",
        "${{TOKEN:=default}}",
        "${{TOKEN:default}}",
        "${{TOKEN-NAME}}",
        "${{}}"
      })
  void givenUnsupportedSecretEnvTemplate_whenResolve_thenThrowException(String command) {
    resolver = NonSecretEnvVarResolver.builder().envVars(Map.of()).strict(false).build();

    assertThatThrownBy(() -> resolver.resolve(command))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Secret environment variable templates only support variable names");
  }

  @ParameterizedTest
  @MethodSource("regularTemplatesProducingSecretTemplates")
  void givenRegularResolutionProducesSecretTemplate_whenResolve_thenConvertToStandardEnvTemplate(
      String command, String expected) {
    resolver =
        NonSecretEnvVarResolver.builder().envVars(Map.of("SECRET_TEMPLATE", "${{SECRET}}")).build();

    var result = resolver.resolve(command);

    assertThat(result).isEqualTo(expected);
  }

  private static Stream<Arguments> regularTemplatesProducingSecretTemplates() {
    return Stream.of(
        arguments("${SECRET_TEMPLATE}", "${SECRET}"),
        arguments("${MISSING:-${{SECRET}}}", "${SECRET}"));
  }

  @ParameterizedTest
  @MethodSource("regularEnvTemplates")
  void givenRegularEnvTemplate_whenResolve_thenDelegateToEnvVarResolver(
      String command, String expected) {
    resolver =
        NonSecretEnvVarResolver.builder().envVars(Map.of("USER", "John")).strict(false).build();

    var result = resolver.resolve(command);

    assertThat(result).isEqualTo(expected);
  }

  private static Stream<Arguments> regularEnvTemplates() {
    return Stream.of(
        arguments("${USER}", "John"),
        arguments("${MISSING:-default}", "default"),
        arguments("${MISSING}", "${MISSING}"));
  }

  @ParameterizedTest
  @MethodSource("jsonSources")
  void givenJsonSource_whenResolveInJson_thenResolveSecretStringLeafNodes(
      String command, String expected) throws IOException {
    resolver = NonSecretEnvVarResolver.builder().envVars(Map.of()).build();

    var result = resolver.resolveInJson(command);
    var json = new ObjectMapper().readTree(result);

    assertThat(json.get("secret").asText()).isEqualTo(expected);
  }

  private static Stream<Arguments> jsonSources() {
    return Stream.of(arguments("{\"secret\":\"${{SECRET}}\"}", "${SECRET}"));
  }
}
