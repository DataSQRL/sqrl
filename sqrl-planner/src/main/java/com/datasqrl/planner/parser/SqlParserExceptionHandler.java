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
package com.datasqrl.planner.parser;

import static com.datasqrl.planner.parser.ParsePosUtil.convertPosition;

import com.datasqrl.error.ErrorHandler;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.error.ErrorMessage;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.flink.table.api.SqlParserException;

@AutoService(ErrorHandler.class)
public class SqlParserExceptionHandler implements ErrorHandler<SqlParserException> {

  @Override
  public ErrorMessage handle(SqlParserException e, ErrorLocation baseLocation) {
    FileLocation location;
    if (e.getCause() instanceof SqlParseException) {
      location = convertPosition(((SqlParseException) e.getCause()).getPos());
    } else {
      location = new FileLocation(1, 1);
    }
    var loc = baseLocation.hasFile() ? baseLocation.atFile(location) : baseLocation;
    return new ErrorMessage.Implementation(
        ErrorLabel.GENERIC, e.getMessage(), loc, ErrorMessage.Severity.FATAL);
  }

  @Override
  public Class getHandleClass() {
    return SqlParserException.class;
  }
}
