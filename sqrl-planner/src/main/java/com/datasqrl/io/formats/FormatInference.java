/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.formats;

import com.datasqrl.io.impl.InputPreview;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor
@NoArgsConstructor
public class FormatInference {

  public static double DEFAULT_CONFIDENCE = 0.95;
  public static long DEFAULT_MAX_TIME_MILLIS = 500;

  private double confidenceThreshold = DEFAULT_CONFIDENCE;
  private long maxTimeMillis = DEFAULT_MAX_TIME_MILLIS;

  public boolean inferConfigFromText(InputPreview preview, Text inference) {
    Iterator<BufferedReader> inputs = preview.getTextPreview().iterator();
    long startTime = System.currentTimeMillis();
    while (inference.getConfidence() < confidenceThreshold && inputs.hasNext() &&
        (System.currentTimeMillis() - startTime < maxTimeMillis)) {
      try (BufferedReader r = inputs.next()) {
        inference.nextSegment(r);
      } catch (IOException e) {
        //Ignore and continue
      }
    }
    return true;
  }

  interface Base {

    double getConfidence();

  }

  interface Text extends Base {

    void nextSegment(@NonNull BufferedReader textInput) throws IOException;

  }


}
