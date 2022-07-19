package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class FieldContainer implements Iterable<Field> {
  Map<String, Field> fieldMap = new LinkedHashMap<>();

  @Override
  public Iterator<Field> iterator() {
    return fieldMap.values().iterator();
  }

  public boolean add(Field field) {
    fieldMap.put(field.getName().getCanonical(), field);
    return true;
  }

  public int size() {
    return fieldMap.size();
  }

  public Stream<Field> stream() {
    return fieldMap.values().stream();
  }

  public Optional<Field> getVisibleByName(Name name) {
    return Optional.ofNullable(fieldMap.get(name.getCanonical()));
  }

  public Field atIndex(int i) {
    return new ArrayList<>(fieldMap.values()).get(i);
  }

  public List<Field> values() {
    return new ArrayList<>(fieldMap.values());
  }
}