package com.datasqrl.vector;

import com.datasqrl.vector.OnnxEmbed.CachedModel;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import com.google.common.cache.CacheLoader;
import org.tribuo.interop.onnx.extractors.OnnxRunner;
import org.tribuo.interop.onnx.extractors.Tokenizer;

public class CacheLoaderImpl extends CacheLoader<String, CachedModel> implements Serializable {
  public static final String TOKENIZER_FILENAME = "tokenizer.json";

    @Override
    public CachedModel load(String s) throws IllegalArgumentException {
      Path modelPath = Paths.get(s);
      Path tokenizerPath = modelPath.getParent().resolve(TOKENIZER_FILENAME);
//      Preconditions.checkArgument(Files.isRegularFile(modelPath) && Files.isReadable(modelPath),
//          "Could not read ONNX model from file [%s]. Check file is readable and correctly mounted.", modelPath);
//      Preconditions.checkArgument(Files.isRegularFile(tokenizerPath) && Files.isReadable(tokenizerPath),
//          "Could not read tokenizer configuration from file [%s]. Check file is readable and correctly mounted.", modelPath);
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
  }
