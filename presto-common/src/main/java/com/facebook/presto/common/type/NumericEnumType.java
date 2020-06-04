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
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;

public class NumericEnumType
        extends AbstractLongType
{
    public final Map<String, Long> entries;
    public static final NumericEnumType MOOD_ENUM = new NumericEnumType(
            "Mood", ImmutableMap.of(
        "HAPPY", Long.valueOf(0), "SAD", Long.valueOf(1))
    );

    public NumericEnumType(String name, Map<String, Long> entries)
    {
        super(parseTypeSignature(name));
        this.entries = entries;
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
