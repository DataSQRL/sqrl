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
package com.datasqrl.error;

import com.google.auto.service.AutoService;
import org.apache.flink.table.api.ValidationException;

@AutoService(ErrorHandler.class)
public class ValidationExceptionHandler implements ErrorHandler<ValidationException> {

  @Override
  public ErrorMessage handle(ValidationException e, ErrorLocation baseLocation) {
    var errCode = ErrorCode.GENERIC;
    var msg = e.getMessage();

    if (msg != null && msg.contains("table source is unbounded")) {
      errCode = ErrorCode.UNBOUNDED_BATCH_SOURCE;
    }

    return new ErrorMessage.Implementation(errCode, msg, baseLocation, ErrorMessage.Severity.FATAL);
  }

  @Override
  public Class<ValidationException> getHandleClass() {
    return ValidationException.class;
  }
}
