package com.datasqrl.util;

public class Money {

  public static double generate(double min, double max, RandomSampler sampler) {
    return fromMoney(sampler.nextLong(toMoney(min),toMoney(max)));
  }

  public static long toMoney(double money) {
    return Math.round(money*100.0);
  }

  public static double fromMoney(long money) {
    return money/100.0;
  }


}
