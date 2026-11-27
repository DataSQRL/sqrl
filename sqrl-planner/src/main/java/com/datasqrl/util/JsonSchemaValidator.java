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
package com.datasqrl.util;

import com.datasqrl.error.ResourceFileUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.dialect.Dialect;
import com.networknt.schema.dialect.Dialects;
import com.networknt.schema.keyword.AnnotationKeyword;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Validates JSON against a JSON schema on the classpath. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class JsonSchemaValidator {

  private static final String TYPE_KEYWORD = "type";
  private static final String UNEVALUATED_PROPERTIES_KEYWORD = "unevaluatedProperties";
  private static final String PROPERTIES_KEYWORD = "properties";

  /**
   * @param instancePointer the JSON pointer of the value that violates the schema, empty for the
   *     root
   * @param message the message of the validator
   * @param keyword the keyword of the schema that the value violates, e.g. {@code type}
   * @param keywordValue the value of that keyword in the schema, e.g. {@code "string"}
   * @param property the property of the value that the violation is about, or null if it is about
   *     the value as a whole
   */
  public record Violation(
      String instancePointer,
      String message,
      String keyword,
      @Nullable JsonNode keywordValue,
      @Nullable String property) {

    /** Whether the value has another type than the given one, which the schema requires. */
    public boolean isNotOfType(String requiredType) {
      return TYPE_KEYWORD.equals(keyword)
          && keywordValue != null
          && requiredType.equals(keywordValue.asText());
    }

    /**
     * Whether the violation is a property that no part of the schema evaluated. The validator
     * reports that for a property that the schema doesn't define, and for every property of an
     * object that is invalid for another reason.
     */
    public boolean isUnevaluatedProperty() {
      return UNEVALUATED_PROPERTIES_KEYWORD.equals(keyword) && property != null;
    }
  }

  /**
   * @param propertyNames the names of the properties that the schema defines, for any of its
   *     objects
   */
  private record CompiledSchema(Schema schema, Set<String> propertyNames) {}

  /**
   * The dialects that a schema can declare with {@code $schema}; a schema that doesn't declare one
   * is read as the first. A keyword that a dialect doesn't define is an annotation, as it is by
   * default, but without the warning that the library logs for each of them: the ODCS schema keeps
   * its server definitions in a group whose members are not keywords.
   */
  private static final List<Dialect> DIALECTS =
      Stream.of(
              Dialects.getDraft202012(),
              Dialects.getDraft201909(),
              Dialects.getDraft7(),
              Dialects.getDraft6(),
              Dialects.getDraft4())
          .map(
              dialect ->
                  Dialect.builder(dialect)
                      .unknownKeywordFactory((keyword, context) -> new AnnotationKeyword(keyword))
                      .build())
          .toList();

  /** The compiled schemas by resource, since compiling a large schema is expensive */
  private static final Map<String, CompiledSchema> SCHEMAS = new ConcurrentHashMap<>();

  /**
   * @param json the JSON to validate
   * @param schemaResource the classpath resource of the schema. A schema that doesn't declare its
   *     dialect with {@code $schema} is read as draft 2020-12
   * @return the violations, empty if the JSON is valid
   * @throws IOException if the schema cannot be parsed
   */
  public static List<Violation> validate(JsonNode json, String schemaResource) throws IOException {
    return compiled(schemaResource).schema().validate(json).stream()
        .map(
            error ->
                new Violation(
                    error.getInstanceLocation().toString(),
                    error.getMessage(),
                    error.getKeyword(),
                    error.getSchemaNode(),
                    error.getProperty()))
        .toList();
  }

  /**
   * @return the names of the properties that the schema defines, for any of its objects
   * @throws IOException if the schema cannot be parsed
   */
  public static Set<String> propertyNames(String schemaResource) throws IOException {
    return compiled(schemaResource).propertyNames();
  }

  private static CompiledSchema compiled(String schemaResource) throws IOException {
    var compiled = SCHEMAS.get(schemaResource);
    if (compiled == null) {
      compiled = compile(schemaResource);
      SCHEMAS.put(schemaResource, compiled);
    }
    return compiled;
  }

  private static CompiledSchema compile(String schemaResource) throws IOException {
    var schemaNode =
        SqrlObjectMapper.INSTANCE.readTree(
            ResourceFileUtil.readResourceFileContents(schemaResource));
    var config = SchemaRegistryConfig.builder().formatAssertionsEnabled(true).build();
    var schema =
        SchemaRegistry.withDialects(DIALECTS, builder -> builder.schemaRegistryConfig(config))
            .getSchema(schemaNode);

    var propertyNames = new HashSet<String>();
    collectPropertyNames(schemaNode, propertyNames);
    return new CompiledSchema(schema, Set.copyOf(propertyNames));
  }

  private static void collectPropertyNames(JsonNode schemaNode, Set<String> propertyNames) {
    var properties = schemaNode.get(PROPERTIES_KEYWORD);
    if (properties != null && properties.isObject()) {
      properties.fieldNames().forEachRemaining(propertyNames::add);
    }
    schemaNode.forEach(child -> collectPropertyNames(child, propertyNames));
  }
}
