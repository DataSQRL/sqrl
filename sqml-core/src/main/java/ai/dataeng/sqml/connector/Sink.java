package ai.dataeng.sqml.connector;

import ai.dataeng.sqml.Session;
import ai.dataeng.sqml.common.QualifiedObjectName;
import ai.dataeng.sqml.relation.TableHandle;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type")
@JsonSubTypes({
    @Type(value = JDBCSink.class, name = "jdbc"),
})
public abstract class Sink {
  public abstract Session createSession();
}
