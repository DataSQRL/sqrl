package org.tribuo.interop.onnx.extractors;

import java.nio.FloatBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.TensorInfo;

public class OnnxRunner {

  public final OrtEnvironment env;
  public final OrtSession session;

  public final int dimension;
  public final int inputSize;

  public OnnxRunner(Path modelPath) throws Exception {
    env = OrtEnvironment.getEnvironment();
    var options = new OrtSession.SessionOptions();

    session = env.createSession(modelPath.toString(), options);
    var outputs = session.getOutputInfo();
    inputSize = session.getInputInfo().size();
    var outInfo = outputs.values().iterator().next();
    var outputZeroTensor = (TensorInfo) outInfo.getInfo();
    var shape = outputZeroTensor.getShape();
    this.dimension = (int)shape[2];
  }

  public OnnxTensor createTensor(int size, long value) throws OrtException {
    var array = new long[size];
    Arrays.fill(array, value);
    return OnnxTensor.createTensor(this.env, new long[][]{array});
  }

  public double[] run(OnnxTensor tokenIds, List<String> tokens) throws Exception {
    var mask = this.createTensor(tokens.size() + 2, 1L);
    var tokenTypes = this.createTensor(tokens.size() + 2, 0L);

    Map<String, OnnxTensor> inputMap = new HashMap(3);
    inputMap.put("input_ids", tokenIds);
    inputMap.put("attention_mask", mask);
    if (inputSize>2) {
      inputMap.put("token_type_ids", tokenTypes);
    }
    var bertOutput = this.session.run(inputMap);

    var clsFeatures = extractCLSVector(bertOutput);
    var tokenFeatures = extractMeanTokenVector(bertOutput, tokens.size(), true);
    var featureValues = new double[dimension];
    for (var i = 0; i < dimension; i++) {
      featureValues[i] = (clsFeatures[i] + tokenFeatures[i]) / 2.0;
    }
    return tokenFeatures;
  }

  private double[] extractCLSVector(OrtSession.Result bertOutput) {
    var tensor = (OnnxTensor) bertOutput.get(0);
    var buffer = tensor.getFloatBuffer();
    return extractFeatures(buffer, dimension);
  }

  private double[] extractMeanTokenVector(OrtSession.Result bertOutput, int numTokens, boolean average) {
    var tensor = (OnnxTensor) bertOutput.get(0);
    var buffer = tensor.getFloatBuffer();
    var featureValues = new double[dimension];
    buffer.position(dimension);
    // iterate the tokens, creating new examples
    for (var i = 0; i < numTokens; i++) {
      addFeatures(buffer, dimension, featureValues);
    }
    if (average) {
      for (var i = 0; i < dimension; i++) {
        featureValues[i] /= numTokens;
      }
    }
    return featureValues;
  }

  private static void addFeatures(FloatBuffer buffer, int bertDim, double[] values) {
    var floatArr = new float[bertDim];
    buffer.get(floatArr);
    for (var i = 0; i < floatArr.length; i++) {
      values[i] += floatArr[i];
    }
  }

  private static double[] extractFeatures(FloatBuffer buffer, int bertDim) {
    var features = new double[bertDim];
    var floatArr = new float[bertDim];
    buffer.get(floatArr);
    for (var i = 0; i < floatArr.length; i++) {
      features[i] = floatArr[i];
    }
    return features;
  }



}
