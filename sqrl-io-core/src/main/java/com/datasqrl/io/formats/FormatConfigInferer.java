package com.datasqrl.io.formats;

import com.datasqrl.io.impl.InputPreview;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
public class FormatConfigInferer {

  public static double DEFAULT_CONFIDENCE = 0.95;
  public static long DEFAULT_MAX_TIME_MILLIS = 500;

  private double confidenceThreshold = DEFAULT_CONFIDENCE;
  private long maxTimeMillis = DEFAULT_MAX_TIME_MILLIS;

  public boolean inferConfig(InputPreview preview, TextLineFormat.ConfigurationInference inferer) {
    Iterator<BufferedReader> inputs = preview.getTextPreview().iterator();
    long startTime = System.currentTimeMillis();
    while (inferer.getConfidence() < confidenceThreshold && inputs.hasNext() &&
        (System.currentTimeMillis() - startTime < maxTimeMillis)) {
      try (BufferedReader r = inputs.next()) {
        inferer.nextSegment(r);
      } catch (IOException e) {
        //Ignore and continue
      }
    }
    return true;
  }


}
