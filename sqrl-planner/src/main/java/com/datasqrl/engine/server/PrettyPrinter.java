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
package com.datasqrl.engine.server;

import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;

public class PrettyPrinter extends DefaultPrettyPrinter {
  private static final long serialVersionUID = 8884340838608574932L;

  public PrettyPrinter() {
    super();
    _objectFieldValueSeparatorWithSpaces = ": ";
    _arrayEmptySeparator = "";
    this.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE);
  }

  @Override
  public DefaultPrettyPrinter createInstance() {
    return new PrettyPrinter();
  }
}
