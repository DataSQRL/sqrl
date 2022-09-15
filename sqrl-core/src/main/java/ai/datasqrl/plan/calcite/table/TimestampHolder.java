package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.plan.calcite.util.IndexMap;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Getter
@AllArgsConstructor
public abstract class TimestampHolder {

    protected boolean candidatesLocked;
    protected List<Candidate> candidates;

    public static class Base extends TimestampHolder {

        public static final Base NONE = new Base(true,Collections.EMPTY_LIST, Collections.EMPTY_LIST);

        private final List<TimestampHolder.Base> dependents;

        private Base(boolean candidatesLocked, List<Candidate> candidates, List<TimestampHolder.Base> dependents) {
            super(candidatesLocked, candidates);
            this.dependents = dependents;
        }

        public Base() {
            this(false, new ArrayList<>(), new ArrayList<>());
        }

        private void addDependent(TimestampHolder.Base dependent) {
            this.dependents.add(dependent);
        }

        public static Base ofDerived(TimestampHolder.Derived derived) {
            if (derived.candidates.isEmpty()) return Base.NONE;
            Base newBase = new Base();
            newBase.candidates.addAll(derived.candidates);
            newBase.addDependent(derived.base);
            derived.base.addDependent(newBase);
            //We created a new query table - candidates must be locked now
            newBase.lockCandidates();
            return newBase;
        }

        public void addCandidate(int columnIndex, int score) {
            Preconditions.checkArgument(!candidatesLocked);
            Preconditions.checkArgument(!candidates.stream().anyMatch(c -> c.index == columnIndex));
            int nextId = candidates.stream().mapToInt(Candidate::getId).max().orElse(0) + 1;
            candidates.add(new Candidate(nextId, columnIndex, score));
        }

        @Override
        public void lockCandidates() {
            super.lockCandidates();
            //Notify all dependents
            dependents.forEach(t -> {
                if (!t.isCandidatesLocked()) t.lockCandidates();
            });
        }

        public void setBestTimestamp() {
            if (hasTimestamp()) return;
            setTimestamp(candidates.stream().max((a,b) -> Integer.compare(a.score,b.score)).get().id);
        }

        public void setTimestamp(int candidateId) {
            Candidate timestamp = candidates.stream().filter(c -> c.id==candidateId).findFirst().orElseThrow(
                    () -> new IllegalArgumentException("Not a valid candidate id: " + candidateId));
            //Remove all others
            candidates = List.of(timestamp);
            candidatesLocked = true;
            dependents.forEach(t -> {
                if (!t.hasTimestamp()) t.setTimestamp(candidateId);
            });
        }

    }

    @Getter
    public static class Derived extends TimestampHolder {

        public static final Derived NONE = new Derived();

        private final TimestampHolder.Base base;

        private Derived() {
            super(true, Collections.EMPTY_LIST);
            this.base = null;
        }

        private Derived(boolean candidatesLocked, List<Candidate> candidates, TimestampHolder.Base base) {
            super(candidatesLocked,candidates);
            this.base = base;
        }

        public Derived(TimestampHolder.Base base) {
            super(base.isCandidatesLocked(), List.copyOf(base.getCandidates()));
            this.base = base;
        }

        public TimestampHolder.Derived remapIndexes(IndexMap map) {
            return new TimestampHolder.Derived(this.candidatesLocked,candidates.stream()
                    .map(c -> c.withIndex(map.map(c.getIndex())))
                    .collect(Collectors.toList()),base);
        }

        public TimestampHolder.Derived propagate(List<Candidate> updatedCandidates) {
            Preconditions.checkArgument(this.candidates.stream().map(c -> c.id).collect(Collectors.toSet())
                    .containsAll(updatedCandidates.stream().map(c -> c.id).collect(Collectors.toSet())));
            //Only used during LP rewriting - hence we assume single dependent for from
            return new TimestampHolder.Derived(this.candidatesLocked, updatedCandidates, this.base);
        }

        public TimestampHolder.Derived fixTimestamp(int columnIndex) {
            return fixTimestamp(columnIndex, columnIndex);
        }

        public TimestampHolder.Derived fixTimestamp(int columnIndex, int newIndex) {
            Preconditions.checkArgument(isCandidate(columnIndex));
            return new Derived(true, List.of(getCandidateByIndex(columnIndex).get().withIndex(newIndex)), base);
        }

    }

    @Override
    public String toString() {
        return "TIMESTAMP="+candidates.toString();
    }

    public void lockCandidates() {
        candidatesLocked = true;
    }

    public boolean isCandidate(int columnIndex) {
        return getCandidateByIndex(columnIndex).isPresent();
    }

    public Predicate<Integer> isCandidatePredicate() {
        return idx -> isCandidate(idx);
    }

    public boolean hasTimestamp() {
        return candidatesLocked && candidates.size()==1;
    }

    public Optional<Candidate> getCandidateByIndex(int index) {
        return candidates.stream().filter(c -> c.index == index).findFirst();
    }

    public Candidate getBestCandidate() {
        Preconditions.checkArgument(!candidates.isEmpty());
        return candidates.stream().max((a,b) -> Integer.compare(a.score,b.score)).get();
    }

    public int getTimestampIndex() {
        Preconditions.checkArgument(hasTimestamp(),"Timestamp has not yet been determined");
        return candidates.get(0).index;
    }


    @Value
    public static class Candidate {

        final int id;
        final int index;
        final int score;

        public Candidate withIndex(int newIndex) {
            return new Candidate(id, newIndex, score);
        }

    }

}
