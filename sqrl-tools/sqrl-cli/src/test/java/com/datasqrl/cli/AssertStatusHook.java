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
package com.datasqrl.cli;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import lombok.Getter;

public class AssertStatusHook implements StatusHook {
  private boolean failed;
  @Getter private String messages = null;
  private Throwable failure;

  @Override
  public void onSuccess(ErrorCollector errors) {
    messages = ErrorPrinter.prettyPrint(errors);
  }

  @Override
  public void onFailure(Throwable e, ErrorCollector errors) {
    messages = ErrorPrinter.prettyPrint(errors);
    failed = true;
    failure = e;
  }

  @Override
  public boolean isSuccess() {
    return !failed;
  }

  @Override
  public boolean isFailed() {
    return failed;
  }

  public Throwable failure() {
    return failure;
  }
}
