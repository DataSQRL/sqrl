/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.input;

import com.datasqrl.schema.type.SqrlTypeVisitor;

public interface SqrlTypeConverter<T> extends SqrlTypeVisitor<T, Void> {

}
