/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible.converters;

import com.datasqrl.schema.type.SqrlTypeVisitor;

public interface SqrlTypeConverter<T> extends SqrlTypeVisitor<T, Void> {}
