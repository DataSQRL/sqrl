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

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.google.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.flink.table.catalog.ObjectIdentifier;

@AllArgsConstructor(onConstructor_ = @Inject)
public class SqlNameUtil {

  private final NameCanonicalizer canonicalizer;

  public NamePath toNamePath(List<String> names) {
    return NamePath.of(names.stream().map(this::toName).collect(Collectors.toList()));
  }

  public Name toName(String name) {
    return name.equals("") ? ReservedName.ALL : canonicalizer.name(name);
  }

  public static ObjectIdentifier toIdentifier(Name name) {
    return toIdentifier(name.getDisplay());
  }

  public static ObjectIdentifier toIdentifier(String name) {
    return ObjectIdentifier.of("default_catalog", "default_database", name);
  }
}
