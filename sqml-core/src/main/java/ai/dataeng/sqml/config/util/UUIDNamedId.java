package ai.dataeng.sqml.config.util;

import lombok.*;

import java.util.UUID;

@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class UUIDNamedId  implements NamedIdentifier {

    private UUID id;

    public static UUIDNamedId of(@NonNull UUID id) {
        return new UUIDNamedId(id);
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
