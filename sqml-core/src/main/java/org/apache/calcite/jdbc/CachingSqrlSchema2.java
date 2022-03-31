/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.jdbc;

import org.apache.calcite.schema.Schema;

/**
 * A no-op wrapper for SimpleCalciteSchema
 *
 * Note: It -could- be possible to treat table paths as arbitrarily nested schemas here except we
 *  cannot deduce the last element of a path to return a table. Alternatively, it -could- be
 *  possible to treat the DOT operator in a table name as a function.
 */
public class CachingSqrlSchema2 extends SimpleCalciteSchema {

  public CachingSqrlSchema2(Schema schema) {
    super(null, schema, "");
  }
}
