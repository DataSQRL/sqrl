/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible.converters;

import com.datasqrl.io.schema.flexible.type.SqrlTypeVisitor;

public interface SqrlTypeConverter<T> extends SqrlTypeVisitor<T, Void> {

}
