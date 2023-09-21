package com.datasqrl.function;

import static com.datasqrl.VectorFunctions.ASCII_TEXT_TEST_EMBED;
import static com.datasqrl.VectorFunctions.CENTER;
import static com.datasqrl.VectorFunctions.COSINE_DISTANCE;
import static com.datasqrl.VectorFunctions.COSINE_SIMILARITY;
import static com.datasqrl.VectorFunctions.EUCLIDEAN_DISTANCE;
import static com.datasqrl.VectorFunctions.ONNX_EMBED;
import static com.datasqrl.VectorFunctions.VEC_TO_DOUBLE;

import com.datasqrl.NamespaceObjectUtil;
import com.datasqrl.canonicalizer.NamePath;
import com.google.auto.service.AutoService;

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
      ONNX_EMBED,
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
