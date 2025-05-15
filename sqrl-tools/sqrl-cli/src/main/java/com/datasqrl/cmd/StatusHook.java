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
package com.datasqrl.cmd;

import com.datasqrl.error.ErrorCollector;
import lombok.Getter;

public interface StatusHook {

  void onSuccess(ErrorCollector errors);

  void onFailure(Throwable e, ErrorCollector errors);

  boolean isSuccess();

  boolean isFailed();

  public static final StatusHook NONE =
      new StatusHook() {
        boolean failed = false;

        @Override
        public void onSuccess(ErrorCollector errors) {}

        @Override
        public void onFailure(Throwable e, ErrorCollector errors) {
          e.printStackTrace();
          failed = true;
        }

        @Override
        public boolean isSuccess() {
          return !failed;
        }

        @Override
        public boolean isFailed() {
          return failed;
        }
      };

  @Getter
  public static class Impl implements StatusHook {

    private boolean failed = false;

    @Override
    public void onSuccess(ErrorCollector errors) {
      failed = false;
    }

    @Override
    public void onFailure(Throwable e, ErrorCollector errors) {
      failed = true;
    }

    @Override
    public boolean isSuccess() {
      return false;
    }

    @Override
    public boolean isFailed() {
      return failed;
    }
  }
}
