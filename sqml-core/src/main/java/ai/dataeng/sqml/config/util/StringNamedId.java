package ai.dataeng.sqml.config.util;

import lombok.*;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class StringNamedId implements NamedIdentifier {

    private String id;

    public static StringNamedId of(@NonNull String id) {
        return new StringNamedId(id.trim());
    }

    @Override
    public String toString() {
        return id;
    }


}
