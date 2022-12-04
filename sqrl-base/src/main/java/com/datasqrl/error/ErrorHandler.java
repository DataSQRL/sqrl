/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

public interface ErrorHandler<E extends Exception> {

  ErrorMessage handle(E e, ErrorEmitter emitter);

  Class getHandleClass();
}
