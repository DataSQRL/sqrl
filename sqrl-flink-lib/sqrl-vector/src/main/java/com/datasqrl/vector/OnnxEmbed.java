package com.datasqrl.vector;

import static com.datasqrl.vector.VectorFunctions.convert;

import ai.onnxruntime.OnnxTensor;
import java.util.List;
import java.util.concurrent.ExecutionException;
import lombok.Value;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import org.apache.flink.table.functions.ScalarFunction;
import org.tribuo.interop.onnx.extractors.OnnxRunner;
import org.tribuo.interop.onnx.extractors.Tokenizer;

/**
 * Computes a vector embedding for the given string based on the provided ONNX vector embedding model.
 * The embedding model should be stored in a directory that is accessible at runtime.
 */
public class OnnxEmbed extends ScalarFunction {


  public LoadingCache<String, CachedModel> models = CacheBuilder.newBuilder().maximumSize(100)
      .build(new CacheLoaderImpl());

  public FlinkVectorType eval(String text, String modelPath) {
    if (text == null || modelPath == null) {
      return null;
    }
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