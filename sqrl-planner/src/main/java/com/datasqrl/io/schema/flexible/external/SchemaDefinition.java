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
package com.datasqrl.io.schema.flexible.external;

import com.datasqrl.io.schema.flexible.input.external.DatasetDefinition;
import com.datasqrl.util.StringNamedId;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class SchemaDefinition implements Serializable {

  public String version;
  public List<DatasetDefinition> datasets;

  public static SchemaDefinition empty() {
    var def = new SchemaDefinition();
    def.datasets = Collections.EMPTY_LIST;
    def.version = StringNamedId.of("1").getId();
    return def;
  }
}
