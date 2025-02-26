package org.tribuo.interop.onnx.extractors;

import ai.onnxruntime.NodeInfo;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.TensorInfo;
import java.nio.FloatBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OnnxRunner {

  public final OrtEnvironment env;
  public final OrtSession session;

  public final int dimension;
  public final int inputSize;

  public OnnxRunner(Path modelPath) throws Exception {
    env = OrtEnvironment.getEnvironment();
    OrtSession.SessionOptions options = new OrtSession.SessionOptions();

    session = env.createSession(modelPath.toString(), options);
    Map<String, NodeInfo> outputs = session.getOutputInfo();
    inputSize = session.getInputInfo().size();
    NodeInfo outInfo = outputs.values().iterator().next();
    TensorInfo outputZeroTensor = (TensorInfo) outInfo.getInfo();
    long[] shape = outputZeroTensor.getShape();
    this.dimension = (int) shape[2];
  }

  public OnnxTensor createTensor(int size, long value) throws OrtException {
    long[] array = new long[size];
    Arrays.fill(array, value);
    return OnnxTensor.createTensor(this.env, new long[][] {array});
  }

  public double[] run(OnnxTensor tokenIds, List<String> tokens) throws Exception {
    OnnxTensor mask = this.createTensor(tokens.size() + 2, 1L);
    OnnxTensor tokenTypes = this.createTensor(tokens.size() + 2, 0L);

    Map<String, OnnxTensor> inputMap = new HashMap(3);
    inputMap.put("input_ids", tokenIds);
    inputMap.put("attention_mask", mask);
    if (inputSize > 2) {
      inputMap.put("token_type_ids", tokenTypes);
    }
    OrtSession.Result bertOutput = this.session.run(inputMap);

    double[] clsFeatures = extractCLSVector(bertOutput);
    double[] tokenFeatures = extractMeanTokenVector(bertOutput, tokens.size(), true);
    double[] featureValues = new double[dimension];
    for (int i = 0; i < dimension; i++) {
      featureValues[i] = (clsFeatures[i] + tokenFeatures[i]) / 2.0;
    }
    return tokenFeatures;
  }

  private double[] extractCLSVector(OrtSession.Result bertOutput) {
    OnnxTensor tensor = (OnnxTensor) bertOutput.get(0);
    FloatBuffer buffer = tensor.getFloatBuffer();
    return extractFeatures(buffer, dimension);
  }

  private double[] extractMeanTokenVector(
      OrtSession.Result bertOutput, int numTokens, boolean average) {
    OnnxTensor tensor = (OnnxTensor) bertOutput.get(0);
    FloatBuffer buffer = tensor.getFloatBuffer();
    double[] featureValues = new double[dimension];
    buffer.position(dimension);
    // iterate the tokens, creating new examples
    for (int i = 0; i < numTokens; i++) {
      addFeatures(buffer, dimension, featureValues);
    }
    if (average) {
      for (int i = 0; i < dimension; i++) {
        featureValues[i] /= numTokens;
      }
    }
    return featureValues;
  }

  private static void addFeatures(FloatBuffer buffer, int bertDim, double[] values) {
    float[] floatArr = new float[bertDim];
    buffer.get(floatArr);
    for (int i = 0; i < floatArr.length; i++) {
      values[i] += floatArr[i];
    }
  }

  private static double[] extractFeatures(FloatBuffer buffer, int bertDim) {
    double[] features = new double[bertDim];
    float[] floatArr = new float[bertDim];
    buffer.get(floatArr);
    for (int i = 0; i < floatArr.length; i++) {
      features[i] = floatArr[i];
    }
    return features;
  }
}
