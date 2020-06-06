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
package com.facebook.presto.common.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;

public class NumericEnumType
        extends AbstractLongType
{
    public final Map<String, Long> entries;

    public NumericEnumType(String name, Map<String, Long> entries)
    {
        super(parseTypeSignature(name));
        this.entries = entries;
    }

    public static List<NumericEnumType> getEnums()
    {
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<Map<String, Map<String, Long>>> typeRef = new TypeReference<Map<String, Map<String, Long>>>() {};
        try {
            Map<String, Map<String, Long>> enumData = mapper.readValue(
                    new File("/Users/dohayon/typedb_enums.json"), typeRef);
            ImmutableList.Builder builder = ImmutableList.builder();
            for (String enumName : enumData.keySet()) {
                Map<String, Long> entries = enumData.get(enumName);
                builder.add(new NumericEnumType(enumName, entries));
            }
            return builder.build();
        }
        catch (IOException e) {
            throw new Error(e);
        }
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return block.getLong(position);
    }

    @Override
    public boolean isComparable()
    {
        return false;
    }

    @Override
    public boolean isOrderable()
    {
        return false;
    }
}
