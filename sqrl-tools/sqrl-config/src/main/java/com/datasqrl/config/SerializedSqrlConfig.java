package com.datasqrl.config;

import java.io.Serializable;

import com.datasqrl.error.ErrorCollector;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import lombok.NonNull;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type")
@JsonSubTypes({
    @Type(value = SqrlConfigCommons.Serialized.class, name = "commons")
})
public interface SerializedSqrlConfig extends Serializable {

  public SqrlConfig deserialize(@NonNull ErrorCollector errors);

}
