/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.functions.text;

import com.datasqrl.NamespaceObjectUtil;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.function.AbstractFunctionModule;
import com.datasqrl.function.StdLibrary;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.text.TextFunctions;
import com.google.auto.service.AutoService;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.table.functions.FunctionDefinition;

@AutoService(StdLibrary.class)
public class StdTextLibraryImpl extends AbstractFunctionModule implements StdLibrary {
  public static final NamePath LIB_NAME = NamePath.of("text");
  public static final List<FunctionDefinition> SQRL_FUNCTIONS = List.of(
      TextFunctions.SPLIT,
      TextFunctions.TEXT_SEARCH,
      TextFunctions.FORMAT,
      TextFunctions.BANNED_WORDS_FILTER
  );

  private static List<NamespaceObject> SQL_FUNCTIONS = List.of(
  );

  public StdTextLibraryImpl() {
    super(ListUtils.union(SQRL_FUNCTIONS.stream().map(NamespaceObjectUtil::createNsObject).collect(Collectors.toList()),SQL_FUNCTIONS));
  }

  public NamePath getPath() {
    return LIB_NAME;
  }

}
