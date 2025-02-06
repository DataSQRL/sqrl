package org.tribuo.interop.onnx.extractors;

import static org.tribuo.interop.onnx.extractors.BERTFeatureExtractor.loadTokenizer;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.tribuo.util.tokens.impl.wordpiece.Wordpiece;
import org.tribuo.util.tokens.impl.wordpiece.WordpieceBasicTokenizer;
import org.tribuo.util.tokens.impl.wordpiece.WordpieceTokenizer;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;

public class Tokenizer {

  private WordpieceTokenizer tokenizer;

  private int maxLength = 512;

  private Map<String, Integer> tokenIDs;
  private String classificationToken;
  private String separatorToken;
  private String unknownToken;

  public Tokenizer(Path tokenizerPath) throws Exception {
    var config = loadTokenizer(tokenizerPath);
    var wordpiece = new Wordpiece(config.tokenIDs.keySet(), config.unknownToken, config.maxInputCharsPerWord);
    this.tokenizer = new WordpieceTokenizer(wordpiece, new WordpieceBasicTokenizer(), config.lowercase, config.stripAccents, Collections.emptySet());
    this.tokenIDs = config.tokenIDs;
    this.unknownToken = config.unknownToken;
    this.classificationToken = config.classificationToken;
    this.separatorToken = config.separatorToken;
  }

  public List<String> tokenize(String data) {
    var tokens = this.tokenizer.split(data);
    if (tokens.size() > this.maxLength - 2) {
      tokens = tokens.subList(0, this.maxLength - 2);
    }
    return tokens;
  }


  public OnnxTensor convertTokens(OrtEnvironment env, List<String> tokens) throws OrtException {
    var size = tokens.size() + 2; // for [CLS] in beginning and [SEP] in the end
    var curTokenIds = new long[size];

    curTokenIds[0] = tokenIDs.get(classificationToken);
    var i = 1;
    for (String token : tokens) {
      var id = tokenIDs.get(token);
      if (id == null) {
        curTokenIds[i] = tokenIDs.get(unknownToken);
      } else {
        curTokenIds[i] = id;
      }
      i++;
    }
    curTokenIds[i] = tokenIDs.get(separatorToken);
    return OnnxTensor.createTensor(env,new long[][]{curTokenIds});
  }


}
