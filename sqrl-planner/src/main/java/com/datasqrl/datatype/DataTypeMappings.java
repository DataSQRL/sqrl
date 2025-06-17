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
package com.datasqrl.datatype;

import com.datasqrl.datatype.DataTypeMapping.SimpleMapper;
import com.datasqrl.flinkrunner.functions.json.jsonb_to_string;
import com.datasqrl.flinkrunner.functions.json.to_jsonb;
import com.datasqrl.flinkrunner.functions.vector.double_to_vector;
import com.datasqrl.flinkrunner.functions.vector.vector_to_double;
import java.util.Optional;

public class DataTypeMappings {

  public static DataTypeMapping.Mapper JSON_STRING =
      new SimpleMapper(new jsonb_to_string(), new to_jsonb());
  public static DataTypeMapping.Mapper JSON_TO_STRING_ONLY =
      new SimpleMapper(new jsonb_to_string(), Optional.empty());
  public static DataTypeMapping.Mapper TO_JSON_ONLY =
      new SimpleMapper(new to_jsonb(), Optional.empty());
  public static DataTypeMapping.Mapper VECTOR_DOUBLE =
      new SimpleMapper(new vector_to_double(), new double_to_vector());
  public static DataTypeMapping.Mapper VECTOR_TO_DOUBLE_ONLY =
      new SimpleMapper(new vector_to_double(), Optional.empty());
  public static DataTypeMapping.Mapper TO_BYTES_ONLY =
      new SimpleMapper(new SerializeToBytes(), Optional.empty());
}
