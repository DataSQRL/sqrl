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
package com.datasqrl.graphql.util;

import io.vertx.core.Vertx;
import java.util.function.Supplier;

/**
 * Vertx-compatible context holder for request tracing. Uses Vertx Context to maintain the request
 * ID throughout the async request lifecycle.
 */
public class RequestContext {
  private static final String REQUEST_ID_KEY = "datasqrl.requestId";
  private static final RequestId UNKNOWN_REQUEST_ID = new RequestId(0L);

  /**
   * Sets the request ID for the current Vertx context.
   *
   * @param requestId the request ID to set
   */
  public static void setRequestId(RequestId requestId) {
    var context = Vertx.currentContext();
    if (context != null) {
      context.put(REQUEST_ID_KEY, requestId);
    }
  }

  /**
   * Gets the request ID for the current Vertx context.
   *
   * @return the request ID, or a default "UNKNOWN" ID if not set
   */
  public static RequestId getRequestId() {
    var context = Vertx.currentContext();
    if (context != null) {
      var requestId = context.<RequestId>get(REQUEST_ID_KEY);
      if (requestId != null) {
        return requestId;
      }
    }
    return UNKNOWN_REQUEST_ID;
  }

  /** Clears the request ID for the current Vertx context. */
  public static void clear() {
    var context = Vertx.currentContext();
    if (context != null) {
      context.remove(REQUEST_ID_KEY);
    }
  }

  /**
   * Executes a runnable with the given request ID in the current context. Note: This doesn't create
   * a new context, just sets the ID in the current one.
   *
   * @param requestId the request ID to use
   * @param runnable the code to execute
   */
  public static void withRequestId(RequestId requestId, Runnable runnable) {
    var previousId = getRequestId();
    try {
      setRequestId(requestId);
      runnable.run();
    } finally {
      if (previousId == UNKNOWN_REQUEST_ID) {
        clear();
      } else {
        setRequestId(previousId);
      }
    }
  }

  /**
   * Executes a supplier with the given request ID in the current context. Note: This doesn't create
   * a new context, just sets the ID in the current one.
   *
   * @param requestId the request ID to use
   * @param supplier the code to execute
   * @return the result of the supplier
   */
  public static <T> T withRequestId(RequestId requestId, Supplier<T> supplier) {
    var previousId = getRequestId();
    try {
      setRequestId(requestId);
      return supplier.get();
    } finally {
      if (previousId == UNKNOWN_REQUEST_ID) {
        clear();
      } else {
        setRequestId(previousId);
      }
    }
  }
}
