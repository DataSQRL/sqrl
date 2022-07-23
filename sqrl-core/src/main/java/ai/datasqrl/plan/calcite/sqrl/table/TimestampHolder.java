package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.plan.calcite.util.IndexMap;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Getter
public class TimestampHolder {

    private boolean candidatesLocked;
    private List<Candidate> candidates;
    private TimestampHolder base = null;

    private TimestampHolder(Candidate timestamp) {
        this.candidatesLocked = true;
        this.candidates = List.of(timestamp);
    }

    public TimestampHolder(TimestampHolder from, List<Candidate> candidates) {
        Preconditions.checkArgument(from.candidates.stream().map(c -> c.id).collect(Collectors.toSet())
                .containsAll(candidates.stream().map(c -> c.id).collect(Collectors.toSet())));
        this.base = from.getBase();
        this.candidatesLocked = from.candidatesLocked;
        this.candidates = candidates;
    }

    public TimestampHolder() {
        this.candidatesLocked = false;
        this.candidates = new ArrayList<>();
//        this.changeListeners = new ArrayList<>();
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

    @Override
    public String toString() {
        return "TIMESTAMP=";
    }

    public TimestampHolder getBase() {
        if (base==null) return this;
        else return base;
    }

    public TimestampHolder remapIndexes(IndexMap map) {
        return new TimestampHolder(this,candidates.stream()
                .map(c -> c.withIndex(map.map(c.getIndex())))
                .collect(Collectors.toList()));
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

    public void addCandidate(int columnIndex, int score) {
        Preconditions.checkArgument(!candidatesLocked);
        Preconditions.checkArgument(!candidates.stream().anyMatch(c -> c.index == columnIndex));
        int nextId = candidates.stream().mapToInt(Candidate::getId).max().orElse(0);
        candidates.add(new Candidate(nextId, columnIndex, score));
    }

    public boolean hasTimestamp() {
        return candidatesLocked && candidates.size()==1;
    }

    private Candidate getCandidateByIndex(int columnIndex) {
        return candidates.stream().filter(c -> c.index == columnIndex).findFirst().orElse(null);
    }

    public TimestampHolder fixTimestamp(int columnIndex) {
        Preconditions.checkArgument(isCandidate(columnIndex));
        if (hasTimestamp()) return this;
        return new TimestampHolder(getCandidateByIndex(columnIndex));
    }

//    public int getTimestamp() {
//        if (isLocked()) return timestampColumn;
//        //else find best candidate
//        Preconditions.checkArgument(!candidateScores.isEmpty());
//        return candidateScores.entrySet().stream()
//                .max((a,b) -> a.getValue().compareTo(b.getValue()))
//                .map(Map.Entry::getKey).get();
//    }
//
//    public void setTimestamp(int columnIndex) {
//        Preconditions.checkArgument(!isLocked());
//        Preconditions.checkArgument(candidateScores.containsKey(columnIndex),"Not a valid timestamp candidate: %s",columnIndex);
//        timestampColumn = columnIndex;
//        candidatesLocked = true;
//        //Notify listeners
//        changeListeners.stream().forEach(l -> l.changeTimestamp(columnIndex));
//    }
//
//    private List<ChangeListener> changeListeners;
//
//
//    public void registerChangeListener(ChangeListener listener) {
//        if (!isLocked()) changeListeners.add(listener);
//    }
//
//    interface ChangeListener {
//
//        public void changeTimestamp(int columnIndex);
//
//    }

}
