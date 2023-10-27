package com.datasqrl.data;

import com.datasqrl.util.RandomSampler;

public class SpikeGenerator {

  public static final double peakErrorMultiplier = 10;

  final RandomSampler sampler;
  final double baseLine;
  final double maxNoise;
  final double peakStdDev;
  final double errorProbability;

  final int maxRampWidth;

  double peakDelta = 0.0;
  int rampWidth = 0;
  int step = 1;

  public SpikeGenerator(RandomSampler sampler, double baseLine, double maxNoise, double peakStdDev,
      int maxRampWidth, double errorProbability) {
    this.sampler = sampler;
    this.baseLine = baseLine;
    this.maxNoise = maxNoise;
    this.peakStdDev = peakStdDev;
    this.errorProbability = errorProbability;
    this.maxRampWidth = maxRampWidth;
  }

  public double nextValue() {
    if (step>rampWidth) {
      //sample next peak
      peakDelta = Math.abs(sampler.nextNormal(0, peakStdDev));
      rampWidth = sampler.nextInt(1, maxRampWidth+1);
      step = -rampWidth;
      System.out.printf("%.2f - %d - %.2f\n", peakDelta, rampWidth, baseLine);
    }
    assert Math.abs(step)<=rampWidth;
    double stdDev = rampWidth/3.0;
    double delta = peakDelta*Math.exp(-(step*step)/(2*stdDev*stdDev));
    double noise = sampler.nextDouble(0, 2*maxNoise)-maxNoise;
    double result = baseLine + delta + noise;
    step++;
    if (sampler.nextDouble(0,1)<errorProbability) {
      return baseLine + noise + sampler.nextDouble(0, 2*peakErrorMultiplier*peakStdDev) - peakErrorMultiplier*peakStdDev;
    } else {
      return result;
    }
  }





}
