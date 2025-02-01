package com.datasqrl.vector;


import java.util.Set;
import org.apache.flink.table.functions.FunctionDefinition;

public class VectorFunctions {

  public static final CosineSimilarity COSINE_SIMILARITY = new CosineSimilarity();
  public static final CosineDistance COSINE_DISTANCE = new CosineDistance();

  public static final EuclideanDistance EUCLIDEAN_DISTANCE = new EuclideanDistance();

  public static final VectorToDouble VEC_TO_DOUBLE = new VectorToDouble();

  public static final DoubleToVector DOUBLE_TO_VECTOR = new DoubleToVector();

  public static final AsciiTextTestEmbed ASCII_TEXT_TEST_EMBED = new AsciiTextTestEmbed();

  public static final Center CENTER = new Center();

  public static final Set<FunctionDefinition> functions = Set.of(
    COSINE_SIMILARITY,
    COSINE_DISTANCE,
    EUCLIDEAN_DISTANCE,
    VEC_TO_DOUBLE,
    DOUBLE_TO_VECTOR,
    ASCII_TEXT_TEST_EMBED,
    CENTER
  );


  public static FlinkVectorType convert(double[] vector) {
    return new FlinkVectorType(vector);
  }

}
