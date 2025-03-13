package com.datasqrl.v2.util;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.v2.tables.SqrlTableFunction;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqrTableFunctionUtil {
    public static Optional<SqrlTableFunction> getTableFunctionFromPath(List<SqrlTableFunction> tableFunctions, NamePath path) {
        final List<SqrlTableFunction> tableFunctionsAtPath = tableFunctions.stream().filter(tableFunction -> tableFunction.getFullPath().equals(path)).collect(Collectors.toList());
        assert (tableFunctionsAtPath.size() <= 1); // no overloading
        return tableFunctionsAtPath.isEmpty() ? Optional.empty() : Optional.of(tableFunctionsAtPath.get(0));
    }

}
