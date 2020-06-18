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
package com.facebook.presto.type;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.NumericEnumType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.metadata.SignatureBuilder;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RoutineCharacteristics;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.RoutineCharacteristics.Determinism.DETERMINISTIC;

public final class EnumCast
{
    private EnumCast() {}

    public static SqlInvokedFunction varcharToEnumCastFunction(NumericEnumType enumType)
    {
        // TODO can we express an enum value in SQL if the cast function doesn't exist yet?
        StringBuilder builder = new StringBuilder();
        builder.append("RETURN CASE input\n");
        for (String key : enumType.entries.keySet()) {
            Long value = enumType.entries.get(key);
            builder.append(String.format("  WHEN %s THEN %d\n", key, value));
        }
        builder.append(String.format("  ELSE FAIL('No key ' || input || ' in enum %s')\n", enumType.getDisplayName()));
        builder.append("END");

        return new SqlInvokedFunction(
                OperatorType.CAST.getFunctionName(),
                ImmutableList.of(new Parameter("input", parseTypeSignature(VARCHAR))),
                enumType.getTypeSignature(),
                String.format("Cast varchar to %s", enumType.getDisplayName()),
                RoutineCharacteristics.builder().setDeterminism(DETERMINISTIC).build(),
                String.format(builder.toString()),
                Optional.empty());
    }


    public static SqlScalarFunction integerToEnumCastFunction(NumericEnumType enumType)
    {
        Signature signature = SignatureBuilder.builder()
                .kind(SCALAR)
                .operatorType(CAST)
                .argumentTypes(parseTypeSignature(StandardTypes.INTEGER))
                .returnType(parseTypeSignature(enumType.getTypeSignature().getBase()))
                .build();
        return SqlScalarFunction.builder(EnumCast.class, CAST)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice.implementation(
                        methodsGroup -> methodsGroup.methods("integerToEnum").withExtraParameters(
                                context -> ImmutableList.of(enumType)))).build();
    }

    @UsedByGeneratedCode
    public static long varcharToEnum(Slice value, NumericEnumType enumType)
    {
        final Long numericValue = enumType.entries.get(value.toStringUtf8());
        if (numericValue == null) {
            throw new PrestoException(
                    INVALID_CAST_ARGUMENT,
                    String.format(
                            "No key '%s' in enum '%s'",
                            value.toStringUtf8(),
                            enumType.getTypeSignature().getBase()));
        }
        return numericValue;
    }

    @UsedByGeneratedCode
    public static long integerToEnum(long value, NumericEnumType enumType)
    {
        if (!enumType.entries.values().contains(value)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT,
                    String.format(
                            "No value '%d' in enum '%s'",
                            value,
                            enumType.getTypeSignature().getBase()));
        }
        return value;
    }
}
