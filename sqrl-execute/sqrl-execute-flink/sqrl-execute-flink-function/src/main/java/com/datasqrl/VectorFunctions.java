package com.datasqrl;

import ai.onnxruntime.OnnxTensor;
import com.datasqrl.calcite.type.VectorType;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.function.SqrlFunction;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.tribuo.interop.onnx.extractors.OnnxRunner;
import org.tribuo.interop.onnx.extractors.Tokenizer;

public class VectorFunctions {

  public static final CosineSimilarity COSINE_SIMILARITY = new CosineSimilarity();
  public static final CosineDistance COSINE_DISTANCE = new CosineDistance();

  public static final EuclideanDistance EUCLIDEAN_DISTANCE = new EuclideanDistance();

  public static final VectorToDouble VEC_TO_DOUBLE = new VectorToDouble();

  public static final OnnxEmbedd ONNX_EMBEDD = new OnnxEmbedd();

  public static final Center CENTER = new Center();

  public static class RandomVector extends ScalarFunction implements SqrlFunction {
    public VectorType[] eval(Integer seed, Integer elements) {
      // Create RealVectors from the input arrays
      ThreadLocalRandom current = ThreadLocalRandom.current();
      return current.doubles(elements).mapToObj(VectorType::new).toArray(
          VectorType[]::new);
    }

    @Override
    public String getDocumentation() {
      return "Generates a random vector with the given seed and length";
    }
  }

  public static class VectorToDouble extends ScalarFunction implements SqrlFunction {
    public double[] eval(VectorType[] vectorType) {
      return Arrays.stream(vectorType)
          .mapToDouble(VectorType::doubleValue)
          .toArray();
    }

    @Override
    public String getDocumentation() {
      return "Converts a vector to a double array";
    }
  }

  public static class CosineDistance extends ScalarFunction implements SqrlFunction {
    public double eval(VectorType[] vectorA, VectorType[] vectorB) {
      // Create RealVectors from the input arrays
      RealVector vA = new ArrayRealVector(VEC_TO_DOUBLE.eval(vectorA), false);
      RealVector vB = new ArrayRealVector(VEC_TO_DOUBLE.eval(vectorB), false);

      // Calculate the cosine similarity
      double dotProduct = vA.dotProduct(vB);
      double normalization = vA.getNorm() * vB.getNorm();

      return dotProduct / normalization;
    }

    @Override
    public String getDocumentation() {
      return "Computes the cosine distance between two vectors";
    }
  }

  public static class CosineSimilarity extends ScalarFunction implements SqrlFunction {
    public double eval(VectorType[] vectorA, VectorType[] vectorB) {
      return 1 - new CosineDistance().eval(vectorA, vectorB);
    }

    @Override
    public String getDocumentation() {
      return "Computes the cosine similarity between two vectors";
    }
  }

  public static class EuclideanDistance extends ScalarFunction implements SqrlFunction {
    public double eval(VectorType[] vectorA, VectorType[] vectorB) {
      // Create RealVectors from the input arrays
      RealVector vA = new ArrayRealVector(VEC_TO_DOUBLE.eval(vectorA), false);
      RealVector vB = new ArrayRealVector(VEC_TO_DOUBLE.eval(vectorB), false);
      return vA.getDistance(vB);
    }

    @Override
    public String getDocumentation() {
      return "Computes the euclidean distance between two vectors";
    }
  }


  public static class OnnxEmbedd extends ScalarFunction implements SqrlFunction {

    public static final String TOKENIZER_FILENAME = "tokenizer.json";

    LoadingCache<String, CachedModel> models = CacheBuilder.newBuilder()
        .maximumSize(100)
        .build(new CacheLoader<String, CachedModel>() {
          @Override
          public CachedModel load(String s) throws IllegalArgumentException {
            Path modelPath = Paths.get(s);
            Path tokenizerPath = modelPath.getParent().resolve(TOKENIZER_FILENAME);
            Preconditions.checkArgument(Files.isRegularFile(modelPath) && Files.isReadable(modelPath),
            "Could not read ONNX model from file [%s]. Check file is readable and correctly mounted.", modelPath);
            Preconditions.checkArgument(Files.isRegularFile(tokenizerPath) && Files.isReadable(tokenizerPath),
                "Could not read tokenizer configuration from file [%s]. Check file is readable and correctly mounted.", modelPath);
            OnnxRunner runner;
            Tokenizer tokenizer;
            try {
              runner = new OnnxRunner(modelPath);
            } catch (Exception e) {
              throw new IllegalArgumentException("Could not instantiate ONNX model: " + modelPath,e);
            }
            try {
              tokenizer = new Tokenizer(tokenizerPath);
            } catch (Exception e) {
              throw new IllegalArgumentException("Could not instantiate tokenizer from: " + tokenizerPath,e);
            }
            return new CachedModel(runner, tokenizer);
          }
        });

    public VectorType[] eval(String text, String modelPath) {
      if (text == null || modelPath == null) return null;
      try {
        CachedModel model = models.get(modelPath);
        return convert(model.embedd(text));
      } catch (ExecutionException e) {
        throw new RuntimeException(e.getCause());
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String getDocumentation() {
      return "Computes a vector embedding for the given string based on the provided model";
    }

    @Value
    public static class CachedModel {
      OnnxRunner runner;
      Tokenizer tokenizer;

      public double[] embedd(String text) throws Exception {
        List<String> tokens = tokenizer.tokenize(text);
        OnnxTensor tokenIds = tokenizer.convertTokens(runner.env, tokens);
        return runner.run(tokenIds, tokens);
      }

    }
  }

  public static class Center extends
      AggregateFunction<VectorType[], CenterAccumulator> implements SqrlFunction {

    @Override
    public Name getFunctionName() {
      return Name.system("avg");
    }

    @Override
    public CenterAccumulator createAccumulator() {
      return new CenterAccumulator();
    }

    @Override
    public VectorType[] getValue(CenterAccumulator acc) {
      if (acc.count == 0) {
        return null;
      } else {
        return convert(acc.get());
      }
    }

    public void accumulate(CenterAccumulator acc, VectorType[] vector) {
      acc.add(VEC_TO_DOUBLE.eval(vector));
    }

    public void retract(CenterAccumulator acc, VectorType[] vector) {
      acc.substract(VEC_TO_DOUBLE.eval(vector));
    }

    public void merge(CenterAccumulator acc, Iterable<CenterAccumulator> iter) {
      for (CenterAccumulator a : iter) {
        acc.addAll(a);
      }
    }

    public void resetAccumulator(CenterAccumulator acc) {
      acc.count = 0;
      acc.sum = null;
    }

    @Override
    public String getDocumentation() {
      return "";
    }
  }

  // mutable accumulator of structured type for the aggregate function
  public static class CenterAccumulator {
    public double[] sum = null;
    public int count = 0;

    public synchronized void add(double[] values) {
      if (count == 0) {
        sum = values.clone();
        count = 1;
      } else {
        Preconditions.checkArgument(values.length==sum.length);
        for (int i = 0; i < values.length; i++) {
          sum[i]+=values[i];
        }
        count++;
      }
    }

    public synchronized void addAll(CenterAccumulator other) {
      if (other.count==0) return;
      if (this.count==0) {
        this.sum = new double[other.sum.length];
      }
      Preconditions.checkArgument(this.sum.length==other.sum.length);
      for (int i = 0; i < other.sum.length; i++) {
        this.sum[i]+=other.sum[i];
      }
      this.count+=other.count;
    }

    public double[] get() {
      Preconditions.checkArgument(count>0);
      double[] result = new double[sum.length];
      for (int i = 0; i < sum.length; i++) {
        result[i]=sum[i]/count;
      }
      return result;
    }

    public synchronized void substract(double[] values) {
      Preconditions.checkArgument(values.length==sum.length);
      for (int i = 0; i < values.length; i++) {
        sum[i]-=values[i];
      }
      count--;
    }
  }



  //Example aggregate function
  public static class WeightedAvgExample extends
      AggregateFunction<Integer, WeightedAvgAccumulator> implements SqrlFunction {

    @Override
    public WeightedAvgAccumulator createAccumulator() {
      return new WeightedAvgAccumulator();
    }

    @Override
    public Integer getValue(WeightedAvgAccumulator acc) {
      if (acc.count == 0) {
        return null;
      } else {
        return acc.sum / acc.count;
      }
    }

    public void accumulate(WeightedAvgAccumulator acc, Integer iValue, Integer iWeight) {
      acc.sum += iValue * iWeight;
      acc.count += iWeight;
    }

    public void retract(WeightedAvgAccumulator acc, Integer iValue, Integer iWeight) {
      acc.sum -= iValue * iWeight;
      acc.count -= iWeight;
    }

    public void merge(WeightedAvgAccumulator acc, Iterable<WeightedAvgAccumulator> it) {
      for (WeightedAvgAccumulator a : it) {
        acc.count += a.count;
        acc.sum += a.sum;
      }
    }

    public void resetAccumulator(WeightedAvgAccumulator acc) {
      acc.count = 0;
      acc.sum = 0;
    }

    @Override
    public String getDocumentation() {
      return "";
    }
  }

  // mutable accumulator of structured type for the aggregate function
  public static class WeightedAvgAccumulator {
    public int sum = 0;
    public int count = 0;
  }

  private static VectorType[] convert(double[] vector) {
    return Arrays.stream(vector).mapToObj(VectorType::new).toArray(
        VectorType[]::new);
  }

}
