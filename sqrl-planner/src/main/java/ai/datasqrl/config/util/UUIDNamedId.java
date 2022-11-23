package ai.datasqrl.config.util;

import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class UUIDNamedId implements NamedIdentifier {

  private UUID id;

  public static UUIDNamedId of(@NonNull UUID id) {
    return new UUIDNamedId(id);
  }

  public static UUIDNamedId of(@NonNull String id) {
    return new UUIDNamedId(UUID.fromString(id));
  }

  public UUID getIdInternal() {
    return id;
  }

  @Override
  public String toString() {
    return id.toString();
  }

  @Override
  public String getId() {
    return id.toString();
  }
}
