/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

import java.io.Serializable;

public interface ErrorHandler<E extends Exception> extends Serializable {

  ErrorMessage handle(E e, ErrorLocation baseLocation);

  Class getHandleClass();
}
