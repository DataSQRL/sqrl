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
package com.datasqrl.config;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorHandler;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorMessage;
import com.google.auto.service.AutoService;
import org.apache.commons.configuration2.ex.ConfigurationException;

@AutoService(ErrorHandler.class)
public class ConfigurationExceptionHandler implements ErrorHandler<ConfigurationException> {

  @Override
  public ErrorMessage handle(ConfigurationException e, ErrorLocation baseLocation) {
    var message = e.getMessage();
    if (e.getCause() != null && e.getCause() != e) {
      message += ": " + e.getCause().getMessage();
    }
    return new ErrorMessage.Implementation(
        ErrorCode.CONFIG_EXCEPTION, message, baseLocation, ErrorMessage.Severity.FATAL);
  }

  @Override
  public Class getHandleClass() {
    return ConfigurationException.class;
  }
}
