/*
 * Copyright 2019 Paulo Lopes.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  and Apache License v2.0 which accompanies this distribution.
 *
 *  The Eclipse Public License is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 *
 *  The Apache License v2.0 is available at
 *  http://www.opensource.org/licenses/apache2.0.php
 *
 *  You may elect to redistribute this code under either of these licenses.
 */
package com.datasqrl;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;

/**
 * The functional interface that represents a lambda
 */
@FunctionalInterface
public interface Lambda {

  /**
   * Responses are asynchronous.
   *
   * @param vertx the vertx instance if needed for more IO
   * @param headers the request headers
   * @param body the request body (null if no body)
   * @return return a future with the buffer to be returned.
   */
  Future<Buffer> call(Vertx vertx, MultiMap headers, Buffer body);
}
