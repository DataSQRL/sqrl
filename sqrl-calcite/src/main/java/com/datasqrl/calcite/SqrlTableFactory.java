package com.datasqrl.calcite;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlNodeList;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;

public interface SqrlTableFactory {

  void createTable(ModuleLoader moduleLoader, List<String> path, RelNode input, List<RelHint> hints,
      Optional<SqlNodeList> opHints,
      List<FunctionParameter> parameters, List<Function> isA, boolean materializeSelf,
      Optional<Supplier<RelNode>> nodeSupplier, ErrorCollector errors, boolean isTest);
}
