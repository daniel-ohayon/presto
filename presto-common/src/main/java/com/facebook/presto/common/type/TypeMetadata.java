package com.facebook.presto.common.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Map;

@Immutable
public class TypeMetadata
{
    private final Map<String, String> enumValues;
    private final String typeKind;

    @JsonCreator
    public TypeMetadata(
            @JsonProperty String typeKind,
            @JsonProperty Map<String, String> enumValues
    ) {
        this.typeKind = typeKind;
        this.enumValues = enumValues;
    }

    @JsonProperty
    public String getTypeKind() {
        return typeKind;
    }

    @JsonProperty
    public Map<String, String> getEnumValues() {
        return enumValues;
    }
}
