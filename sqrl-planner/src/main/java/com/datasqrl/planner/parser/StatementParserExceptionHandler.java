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

import com.datasqrl.error.ErrorHandler;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorMessage;
import com.google.auto.service.AutoService;

@AutoService(ErrorHandler.class)
public class StatementParserExceptionHandler implements ErrorHandler<StatementParserException> {

  @Override
  public ErrorMessage handle(
      StatementParserException e, ErrorLocation baseLocation, String messagePrefix) {
    return new ErrorMessage.Implementation(
        e.errorLabel,
        e.getMessage(),
        baseLocation.hasFile()
            ? baseLocation.atFile(baseLocation.getFileLocation().add(e.fileLocation))
            : baseLocation.atFile(e.fileLocation),
        ErrorMessage.Severity.FATAL);
  }

  @Override
  public ErrorMessage handle(StatementParserException e, ErrorLocation baseLocation) {
    return handle(e, baseLocation, null);
  }

  @Override
  public Class<StatementParserException> getHandleClass() {
    return StatementParserException.class;
  }
}
