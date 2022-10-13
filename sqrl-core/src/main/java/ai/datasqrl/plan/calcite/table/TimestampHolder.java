package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.plan.calcite.util.IndexMap;
import com.google.common.base.Preconditions;
import lombok.*;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Getter
public abstract class TimestampHolder<C extends TimestampHolder.Candidate> {

    protected List<C> candidates;

    protected TimestampHolder(List<C> candidates) {
        this.candidates = candidates;
    }

    public Optional<C> getOptCandidateByIndex(int index) {
        return candidates.stream().filter(c -> c.index == index).findFirst();
    }

    public boolean isCandidate(int columnIndex) {
        return getOptCandidateByIndex(columnIndex).isPresent();
    }

    public C getCandidateByIndex(int index) {
        return getOptCandidateByIndex(index).get();
    }

    public Predicate<Integer> isCandidatePredicate() {
        return idx -> isCandidate(idx);
    }

    public C getBestCandidate() {
        Preconditions.checkArgument(!candidates.isEmpty());
        return candidates.stream().max((a,b) -> Integer.compare(a.getScore(),b.getScore())).get();
    }

    public boolean hasFixedTimestamp() {
        return candidates.size()==1;
    }

    public C getTimestampCandidate() {
        Preconditions.checkArgument(hasFixedTimestamp());
        return candidates.get(0);
    }

    public boolean hasCandidates() {
        return !candidates.isEmpty();
    }

    public List<Integer> getCandidateIndexes() {
        return candidates.stream().map(Candidate::getIndex).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "TIMESTAMP="+candidates.toString();
    }

    protected Map<Integer,C> candidatesByIndex() {
        return candidates.stream().collect(Collectors.toMap(Candidate::getIndex, Function.identity()));
    }

    @AllArgsConstructor
    @Getter
    @EqualsAndHashCode
    @ToString
    public static abstract class Candidate {

        final int index;

        public abstract int getScore();

    }

    @Getter
    public static class Base extends TimestampHolder<TimestampHolder.Base.Candidate> {

        public static final TimestampHolder.Base NONE = new TimestampHolder.Base(true, List.of());

        private boolean candidatesLocked; //If true, candidates can no longer be added

        public Base() {
            this(false,new ArrayList<>());
        }

        private Base(boolean candidatesLocked, List<Candidate> candidates) {
            super(candidates);
            this.candidatesLocked = candidatesLocked;
        }

        public static Base ofDerived(TimestampHolder.Derived derived) {
            Base base = new Base();
            base.candidatesLocked = true; //derived bases are always locked
            derived.candidates.forEach(base::addCandidate);
            return base;
        }

        private void addCandidate(TimestampHolder.Derived.Candidate derivedCand) {
            Candidate cand = new Candidate(derivedCand.getIndex(), derivedCand.getScore());
            cand.dependents.addAll(derivedCand.basedOn);
            derivedCand.basedOn.forEach(d -> d.dependents.add(cand));
            candidates.add(cand);
        }


        public void addCandidate(int columnIndex, int score) {
            Preconditions.checkArgument(!candidatesLocked);
            Preconditions.checkArgument(!candidates.stream().anyMatch(c -> c.index == columnIndex));
            candidates.add(new Candidate(columnIndex, score));
        }

        public TimestampHolder.Derived getDerived() {
            this.candidatesLocked = true; //Once we start deriving, we need to lock down the timestamp candidates
            return new Derived(candidates.stream().map(c -> new Derived.Candidate(c.getIndex(),List.of(c)))
                    .collect(Collectors.toList()));
        }

        public boolean hasFixedTimestamp() {
            return super.hasFixedTimestamp() && candidatesLocked;
        }


        @Getter
        public class Candidate extends TimestampHolder.Candidate {

            final int score;
            final Set<TimestampHolder.Base.Candidate> dependents = new HashSet<>();

            private Candidate(int index, int score) {
                super(index);
                this.score = score;
            }

            public void fixAsTimestamp() {
                candidatesLocked = true;
                //Eliminate all other candidates
                for (Candidate c : new ArrayList<>(candidates)) {
                    if (c!=this) c.eliminate();
                }
            }

            private void eliminate() {
                if (candidates.remove(this)) {
                    //Notify all dependents to remove all well
                    dependents.forEach(Candidate::eliminate);
                }
            }

        }
    }


    public static class Derived extends TimestampHolder<TimestampHolder.Derived.Candidate> {

        public static final Derived NONE = new Derived(List.of());

        private Derived(List<Candidate> candidates) {
            super(candidates);
        }

        public Derived restrictTo(List<Candidate> subset) {
            Preconditions.checkArgument(subset.stream().allMatch(c -> candidates.contains(c)),"Invalid candidates");
            candidates.forEach(c -> {
                if (!subset.contains(c)) c.eliminate();
            });
            return new Derived(subset);
        }

        public Derived remapIndexes(IndexMap map) {
            return new TimestampHolder.Derived(candidates.stream()
                    .map(c -> c.withIndex(map.map(c.getIndex())))
                    .collect(Collectors.toList()));
        }

        public Derived union(Derived other) {
            Map<Integer,Candidate> byIndex = this.candidatesByIndex(), otherByIndex = other.candidatesByIndex();
            otherByIndex.forEach((idx, cand) -> byIndex.merge(idx, cand, (c1, c2) -> {
                assert c1.index==c2.index;
                return new Candidate(c1.index, ListUtils.union(c1.basedOn,c2.basedOn));
            }));
            return new Derived(ImmutableList.copyOf(byIndex.values()));
        }

        public static class Candidate extends TimestampHolder.Candidate {

            final List<Base.Candidate> basedOn;

            public Candidate(int index, @NonNull List<Base.Candidate> basedOn) {
                super(index);
                Preconditions.checkArgument(!basedOn.isEmpty());
                this.basedOn = basedOn;
            }

            public Candidate withIndex(int newIndex) {
                return new Candidate(newIndex, basedOn);
            }

            public Derived fixAsTimestamp() {
                basedOn.forEach(Base.Candidate::fixAsTimestamp);
                return new Derived(List.of(this));
            }

            private void eliminate() {
                basedOn.forEach(Base.Candidate::eliminate);
            }

            @Override
            public int getScore() {
                return basedOn.stream().map(Base.Candidate::getScore).max(Integer::compareTo).get();
            }

            //Derived candidates are equal iff their dependents are

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Candidate candidate = (Candidate) o;
                return basedOn.equals(candidate.basedOn);
            }

            @Override
            public int hashCode() {
                return Objects.hash(basedOn);
            }
        }

    }


}
