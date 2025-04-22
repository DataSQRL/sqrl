package com.datasqrl.functions.vector;

import static com.datasqrl.types.vector.functions.VectorFunctions.ASCII_TEXT_TEST_EMBED;
import static com.datasqrl.types.vector.functions.VectorFunctions.CENTER;
import static com.datasqrl.types.vector.functions.VectorFunctions.COSINE_DISTANCE;
import static com.datasqrl.types.vector.functions.VectorFunctions.COSINE_SIMILARITY;
import static com.datasqrl.types.vector.functions.VectorFunctions.DOUBLE_TO_VECTOR;
import static com.datasqrl.types.vector.functions.VectorFunctions.EUCLIDEAN_DISTANCE;
import static com.datasqrl.types.vector.functions.VectorFunctions.VEC_TO_DOUBLE;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.table.functions.FunctionDefinition;

import com.datasqrl.NamespaceObjectUtil;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.function.AbstractFunctionModule;
import com.datasqrl.function.StdLibrary;
import com.google.auto.service.AutoService;

@AutoService(StdLibrary.class)
public class StdVectorLibraryImpl extends AbstractFunctionModule implements StdLibrary {

  public static final NamePath LIB_NAME = NamePath.of("vector");

  public static final List<FunctionDefinition> SQRL_FUNCTIONS = List.of(
      COSINE_SIMILARITY,
      COSINE_DISTANCE,
      EUCLIDEAN_DISTANCE,
      VEC_TO_DOUBLE,
      DOUBLE_TO_VECTOR,
      ASCII_TEXT_TEST_EMBED,
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
