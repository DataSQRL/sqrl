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

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.util.FileUtil;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.Getter;

@AllArgsConstructor
@Getter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class APISourceImpl implements APISource {

  @Include Name name;
  String schemaDefinition;

  public static APISource of(String schemaDefinition) {
    return new APISourceImpl(Name.system("schema"), schemaDefinition.replaceAll("\t", "  "));
  }

  @Override
  public String toString() {
    return name.toString();
  }

  public static APISource of(
      String filePath, NameCanonicalizer canonicalizer, ResourceResolver resolver) {
    return new APISourceImpl(
        canonicalizer.name(FileUtil.separateExtension(FileUtil.getFileName(filePath)).getKey()),
        FileUtil.readFile(resolver.resolveFile(NamePath.of(filePath)).get()));
  }

  @Override
  public APISource clone(String schema) {
    return new APISourceImpl(name, schema);
  }
}
