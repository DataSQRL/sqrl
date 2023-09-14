package com.datasqrl.calcite;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

public interface SqrlTableFactory {

  void createTable(List<String> path, RelNode input, List<RelHint> hints,
      boolean setFieldNames, Optional<SqlNodeList> opHints,
      List<FunctionParameter> parameters, List<Function> isA, boolean materializeSelf,
      Optional<Supplier<RelNode>> nodeSupplier);

  Relationship createParent(NamePath path, SQRLTable parent, SQRLTable sqrlTable);
}
