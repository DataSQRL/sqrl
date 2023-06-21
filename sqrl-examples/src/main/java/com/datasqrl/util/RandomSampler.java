package com.datasqrl.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Value;


@Value
public class RandomSampler {

  private Random random;


  public double nextNormal(double mean, double stdDev) {
    return random.nextGaussian()*stdDev + mean;
  }

  public double nextPositiveNormal(double mean, double stdDev) {
    return Math.max(0, nextNormal(mean, stdDev));
  }

  public int nextInt(int minInclusive, int maxExclusive) {
    int delta = maxExclusive-minInclusive;
    return random.nextInt(delta)+minInclusive;
  }

  public long nextLong(long minInclusive, long maxExclusive) {
    return random.longs(1,minInclusive,maxExclusive).findFirst().getAsLong();
  }

  public double nextDouble(double minInclusive, double maxExclusive) {
    return random.nextDouble()*(maxExclusive-minInclusive) + minInclusive;
  }

  public boolean flipCoin(double likelihood) {
    return random.nextDouble()<likelihood;
  }

  public Set<Integer> withoutReplacement(int number, int minInclusive, int maxExclusive) {
    int delta = maxExclusive-minInclusive;
    Preconditions.checkArgument(number <= delta);
    if (number == delta) {
      return ContiguousSet.closedOpen(minInclusive,maxExclusive);
    }
    Set<Integer> result = new HashSet<>();
    while (result.size()<number) {
      result.add(nextInt(minInclusive,maxExclusive));
    }
    return result;
  }

  public<E> Set<E> withoutReplacement(int number, List<E> elements) {
    return withoutReplacement(number,0, elements.size()).stream()
        .map(elements::get).collect(Collectors.toSet());
  }

  public<E> List<E> withReplacement(int number, List<E> elements) {
    return IntStream.range(0,number).map(i -> random.nextInt(elements.size()))
        .mapToObj(elements::get).collect(Collectors.toList());
  }

  public<E> E next(List<E> elements) {
    return elements.get(nextInt(0, elements.size()));
  }

  public UUID nextUUID() {
    return new UUID(random.nextLong(), random.nextLong());
  }

  public Instant nextTimestamp(Instant base, long bound, ChronoUnit timeUnit) {
    long boundMillis = bound;
    switch (timeUnit) {
      case DAYS:
        boundMillis *= 24;
      case HOURS:
        boundMillis *= 60;
      case MINUTES:
        boundMillis *= 60;
      case SECONDS:
        boundMillis *= 1000;
      case MILLIS:
        break;
      default:
        throw new UnsupportedOperationException("Unsupported time unit: " + timeUnit);
    }
    long sample = nextLong(0,boundMillis);
    return base.plus(sample,ChronoUnit.MILLIS);
  }

  public Instant nextTimestamp(Instant startInclusive, Instant endTimeExclusive) {
    Preconditions.checkArgument(endTimeExclusive.compareTo(startInclusive)>0,
        "End time %s needs to be after start time %s", endTimeExclusive, startInclusive);
    long deltaMillis = endTimeExclusive.toEpochMilli() - startInclusive.toEpochMilli();
    long sample = nextLong(0,deltaMillis);
    return startInclusive.plus(sample,ChronoUnit.MILLIS);
  }

  public static final long MILLIS_IN_SEC = 1000;
  public static final long MILLIS_IN_MINUTE = 60*MILLIS_IN_SEC;
  public static final long MILLIS_IN_HOUR = 60 * MILLIS_IN_MINUTE;
  public static final long MILLIS_IN_DAY = 24 * MILLIS_IN_HOUR;

}
