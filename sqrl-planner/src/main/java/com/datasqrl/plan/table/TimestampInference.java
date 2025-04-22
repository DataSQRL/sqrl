/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.datasqrl.plan.util.IndexMap;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * {@link TimestampInference} contains all the candidate timestamp columns for a relation, i.e. either an
 * imported table or one defined by a query (i.e. {@link org.apache.calcite.rel.RelNode}).
 *
 * We create a graph of timestamp column dependencies so that we can defer timestamp column determination
 * and pick timestamp columns based on table definitions in the script. The {@link TimestampInference.Candidate}
 * objects keep track of what candidates they depend on and what candidates depend on them (i.e. parents and children,
 * respectively).
 *
 * When a timestamp column is fixed, we fix the parent timestamp columns all the way to the source table and
 * eliminate any other candidates that are no longer viable as a result (since source tables can only have one
 * timestamp column).
 *
 * We distinguish between "derived" {@link TimestampInference} and "base". The former is used during the logical plan
 * rewriting process in {@link com.datasqrl.plan.rules.SQRLLogicalPlanRewriter} for the intermediate RelNodes in the
 * query tree whereas the latter is used on the resulting tables.
 *
 *
 */
@EqualsAndHashCode
@AllArgsConstructor
public class TimestampInference {

  private final Map<Integer, Candidate> candidates = new HashMap<>();
  @Getter
  private final boolean isDerived;

  public Optional<Candidate> getOptCandidateByIndex(int index) {
    return Optional.ofNullable(candidates.get(index));
  }

  public boolean isCandidate(int columnIndex) {
    return candidates.containsKey(columnIndex);
  }

  public Candidate getCandidateByIndex(int index) {
    Preconditions.checkArgument(isCandidate(index));
    return candidates.get(index);
  }

  public Iterable<Candidate> getCandidates() {
    return candidates.values();
  }

  public Predicate<Integer> isCandidatePredicate() {
    return idx -> isCandidate(idx);
  }

  public Candidate getBestCandidate() {
    Preconditions.checkArgument(!candidates.isEmpty(),
        "Could not find timestamp candidate.");
    return candidates.values().stream().max(Comparator.comparingInt(Candidate::getScore)).get();
  }

  public boolean hasFixedTimestamp() {
    return candidates.size() == 1;
  }

  public Candidate getTimestampCandidate() {
    Preconditions.checkArgument(hasFixedTimestamp());
    return Iterables.getOnlyElement(candidates.values());
  }

  public boolean hasCandidates() {
    return !candidates.isEmpty();
  }

  public Set<Integer> getCandidateIndexes() {
    return candidates.keySet();
  }

  @Override
  public String toString() {
    return "TIMESTAMP=" + candidates.values().toString();
  }

  protected Map<Integer, Candidate> candidatesByIndex() {
    return candidates;
  }

  public void selectTimestamp() {
    if (candidates.size()>1) {
      var best = getBestCandidate();
      if (best.parents.isEmpty()) {
        best.fixAsTimestamp();
      } else {
        //Just delete the siblings
        candidates.clear();
        candidates.put(best.index, best);
      }
    }
  }

  private void addCandidateInternal(Candidate candidate) {
    Preconditions.checkArgument(candidate.getTimestamps()==this);
    Preconditions.checkArgument(!candidates.containsKey(candidate.index));
    candidates.put(candidate.index,candidate);
  }

  private Candidate addCandidate(int index, int score) {
    var candidate = new Candidate(index, score, List.of(), new ArrayList<>());
    addCandidateInternal(candidate);
    return candidate;
  }

  private Candidate addCandidate(int index, Candidate... basedOn) {
    Preconditions.checkArgument(basedOn.length>0);
    var isDerived = Arrays.stream(basedOn).allMatch(Candidate::isDerived);
    var isBase = Arrays.stream(basedOn).noneMatch(Candidate::isDerived);
    Preconditions.checkArgument(isDerived ^ isBase, "Invalid input timestamps");
    //Reference parents if derived, else base becomes parent

    var parents = isDerived?Arrays.stream(basedOn).flatMap(c -> c.children.stream()).collect(Collectors.toUnmodifiableList()):
            Arrays.stream(basedOn).collect(Collectors.toUnmodifiableList());
    int maxScore = Arrays.stream(basedOn).map(Candidate::getScore).max(Integer::compareTo).get();
    var candidate = new Candidate(index, maxScore, parents, new ArrayList<>());
    addCandidateInternal(candidate);
    return candidate;
  }

  public TimestampInference asDerived() {
    Preconditions.checkArgument(!isDerived());
    var timestamp = new TimestampInference(true);
    this.candidates.values().forEach(c -> timestamp.addCandidate(c.index, c));
    return timestamp;
  }

  public TimestampInference remapIndexes(IndexMap map) {
    Preconditions.checkArgument(isDerived());
    var timestamp = new TimestampInference(true);
    this.candidates.values().forEach(c -> timestamp.addCandidate(map.map(c.index), c));
    return timestamp;
  }

  public TimestampInference finalizeAsBase() {
    Preconditions.checkArgument(isDerived());
    var timestamp = new TimestampInference(false);
    this.candidates.values().forEach(c -> timestamp.addCandidate(c.index, c));
    timestamp.candidates.values().forEach(Candidate::registerOnParents);
    if (timestamp.hasFixedTimestamp()) { //Fix the timestamp
      Iterables.getOnlyElement(timestamp.candidates.values()).fixAsTimestamp();
    }
    return timestamp;
  }

  private static class Builder {

    protected final TimestampInference timestamps;

    private Builder(boolean isDerived) {
      timestamps = new TimestampInference(isDerived);
    }

  }

  public static DerivedBuilder buildDerived() {
    return new DerivedBuilder();
  }

  public static TimestampInference ofDerived(Candidate basedOn) {
    return ofDerived(basedOn.index, basedOn);
  }

  public static TimestampInference ofDerived(int index, Candidate... basedOn) {
    Preconditions.checkArgument(basedOn.length>0);
    Arrays.stream(basedOn).forEach(Candidate::fixAsTimestamp);
    return buildDerived().add(index,basedOn).build();
  }

  public static ImportBuilder buildImport() {
    return new ImportBuilder();
  }

  public static class DerivedBuilder extends Builder {

    private DerivedBuilder() {
      super(true);
    }

    public DerivedBuilder add(int index, Candidate... basedOn) {
      timestamps.addCandidate(index, basedOn);
      return this;
    }

    public boolean hasCandidates() {
      return timestamps.hasCandidates();
    }

    public TimestampInference build() {
      return timestamps;
    }

  }

  public static class ImportBuilder extends Builder {

    private ImportBuilder() {
      super(false);
    }

    public ImportBuilder addImport(int index, int score) {
      timestamps.addCandidate(index, score);
      return this;
    }

    public TimestampInference build() {
      return timestamps;
    }

  }

  @AllArgsConstructor
  @Getter
  public class Candidate {

    int index;
    int score;
    List<Candidate> parents;
    List<Candidate> children;


    public void fixAsTimestamp() {
      if (parents.isEmpty()) {
        for (Candidate c : new ArrayList<>(candidates.values())) {
          if (this != c) {
              //We can only have one timestamp column on source/imported tables
              c.eliminate();
            }
          }
      } else {
        parents.forEach(Candidate::fixAsTimestamp);
      }
    }

    public boolean isDerived() {
      return TimestampInference.this.isDerived();
    }

    private void eliminate() {
      children.forEach(Candidate::eliminate);
      var removed = candidates.remove(this.index)!=null;
      Preconditions.checkArgument(removed);
    }

    private void registerOnParents() {
      parents.forEach(parent -> parent.children.add(this));
    }

    private TimestampInference getTimestamps() {
      return TimestampInference.this;
    }

    @Override
	public String toString() {
      return "TS#" + index;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
		return true;
	}
      if (o == null || getClass() != o.getClass()) {
		return false;
	}
      var candidate = (Candidate) o;
      return getTimestamps() == candidate.getTimestamps() && index == candidate.index;
    }

    @Override
    public int hashCode() {
      return Objects.hash(getTimestamps(), index);
    }
  }


}
