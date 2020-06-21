/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.client;

import com.facebook.presto.common.type.EnumType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeMetadata;
import com.facebook.presto.common.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.sun.security.ntlm.Client;

import javax.annotation.concurrent.Immutable;

import java.util.Map;

import static java.util.Objects.requireNonNull;

@Immutable
public class Column
{
    private final String name;
    private final String type;
    private final ClientTypeSignature typeSignature;
    private final TypeMetadata typeMetadata;

    public Column(String name, Type type)
    {
        this(
                name,
                type.getTypeSignature().toString(),
                new ClientTypeSignature(type.getTypeSignature()),
                type.getTypeMetadata()
        );
    }

    public Column(String name, TypeSignature signature)
    {
        this(name, signature.toString(), new ClientTypeSignature(signature), null);
    }

    @JsonCreator
    public Column(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("typeSignature") ClientTypeSignature typeSignature,
            @JsonProperty("typeMetadata") TypeMetadata typeMetadata
    )
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.typeSignature = typeSignature;
        this.typeMetadata = typeMetadata;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public ClientTypeSignature getTypeSignature()
    {
        return typeSignature;
    }

    @JsonProperty
    public TypeMetadata getTypeMetadata() { return typeMetadata; }
}
