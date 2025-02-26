/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.type.basic;

import com.datasqrl.schema.type.Type;
import java.util.List;

public interface BasicType<JavaType> extends Type, Comparable<BasicType<?>> {
  // First entry is the preferred name, the remaining are for backwards compatibility
  String getName();

  List<String> getNames();

  TypeConversion<JavaType> conversion();
}
