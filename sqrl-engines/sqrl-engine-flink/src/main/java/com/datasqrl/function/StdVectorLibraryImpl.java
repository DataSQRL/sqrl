package com.datasqrl.function;

import static com.datasqrl.VectorFunctions.CENTER;
import static com.datasqrl.VectorFunctions.COSINE_DISTANCE;
import static com.datasqrl.VectorFunctions.COSINE_SIMILARITY;
import static com.datasqrl.VectorFunctions.EUCLIDEAN_DISTANCE;
import static com.datasqrl.VectorFunctions.ONNX_EMBEDD;
import static com.datasqrl.VectorFunctions.VEC_TO_DOUBLE;

import com.datasqrl.NamespaceObjectUtil;
import com.datasqrl.VectorFunctions;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SimpleCallTransform;
import com.datasqrl.calcite.function.RuleTransform;
import com.datasqrl.calcite.type.VectorType;
import com.datasqrl.canonicalizer.NamePath;
import com.google.auto.service.AutoService;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@AutoService(StdLibrary.class)
public class StdVectorLibraryImpl extends AbstractFunctionModule implements StdLibrary {

  public static final NamePath LIB_NAME = NamePath.of("vector");

  public static final List<SqrlFunction> SQRL_FUNCTIONS = List.of(
      COSINE_SIMILARITY,
      COSINE_DISTANCE,
      EUCLIDEAN_DISTANCE,
      VEC_TO_DOUBLE,
      ONNX_EMBEDD,
      CENTER
  );

  public StdVectorLibraryImpl() {
    super(SQRL_FUNCTIONS.stream()
        .map(NamespaceObjectUtil::createNsObject)
        .collect(Collectors.toList()));
  }

  @Override
  public NamePath getPath() {
    return LIB_NAME;
  }

}
