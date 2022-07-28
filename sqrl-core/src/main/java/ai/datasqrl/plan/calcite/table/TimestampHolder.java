package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.plan.calcite.util.IndexMap;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@AllArgsConstructor
public abstract class TimestampHolder {

    protected boolean candidatesLocked;
    protected final List<Candidate> candidates;



    public static class Base extends TimestampHolder {

        private final List<TimestampHolder.Base> dependents;

        public Base() {
            super(false, new ArrayList<>());
            this.dependents = new ArrayList<>();
        }

        public static Base ofDerived(TimestampHolder.Derived derived) {
            Base newBase = new Base();
            newBase.candidates.addAll(derived.candidates);
            newBase.dependents.add(derived.base);
            derived.base.dependents.add(newBase);
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
    }

    @Getter
    public static class Derived extends TimestampHolder {

        private final TimestampHolder.Base base;

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
            Preconditions.checkArgument(isCandidate(columnIndex));
            if (hasTimestamp()) return this;
            return new Derived(true, List.of(getCandidateByIndex(columnIndex)), base);
        }

    }

    @Override
    public String toString() {
        return "TIMESTAMP="+candidates.toString();
    }

    public boolean isCandidateLocked() {
        return candidatesLocked;
    }

    public void lockCandidates() {
        candidatesLocked = true;
    }

    public boolean isCandidate(int columnIndex) {
        return getCandidateByIndex(columnIndex)!=null;
    }

    public boolean hasTimestamp() {
        return candidatesLocked && candidates.size()==1;
    }

    protected Candidate getCandidateByIndex(int columnIndex) {
        return candidates.stream().filter(c -> c.index == columnIndex).findFirst().orElse(null);
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
