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
package com.datasqrl.packager.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.graphql.SqrlObjectMapper;
import com.datasqrl.graphql.config.ServerConfigUtil;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

class ServerConfigTemplateTest {
  private static final File TEMPLATE = new File("src/main/resources/templates/server-config.json");

  private static ObjectMapper mapper = SqrlObjectMapper.MAPPER;

  @SuppressWarnings("unchecked")
  @Test
  @SneakyThrows
  void test() {
    var original = mapper.readValue(TEMPLATE, Map.class);
    var afterParsing = ServerConfigUtil.fromConfigMap(original);
    assertThat(mapper.convertValue(afterParsing, Map.class))
        .usingRecursiveComparison()
        .withComparatorForType(numberComparatorIgnoringType(), Number.class)
        .isEqualTo(original);
  }

  private static Comparator<Number> numberComparatorIgnoringType() {
    return (n1, n2) -> {
      if (n1 == null && n2 == null) return 0;
      if (n1 == null) return -1;
      if (n2 == null) return 1;
      return Double.compare(n1.doubleValue(), n2.doubleValue());
    };
  }

  @SneakyThrows
  public static void main(String[] args) {
    var original = mapper.readValue(TEMPLATE, Map.class);
    var afterParsing = ServerConfigUtil.fromConfigMap(original);

    if (!Objects.equals(original, mapper.convertValue(afterParsing, Map.class))) {
      mapper
          .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
          .writerWithDefaultPrettyPrinter()
          .writeValue(TEMPLATE, afterParsing);
    }
  }
}
