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
import com.facebook.presto.common.type.NumericEnumType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.metadata.SignatureBuilder;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Signature;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;

public final class EnumCast
{
    private EnumCast() {}

    public static SqlScalarFunction varcharToEnumCastFunction(NumericEnumType enumType)
    {
        Signature signature = SignatureBuilder.builder()
                .kind(SCALAR)
                .operatorType(CAST)
                .argumentTypes(parseTypeSignature(StandardTypes.VARCHAR))
                .returnType(parseTypeSignature(enumType.getTypeSignature().getBase()))
                .build();
        return SqlScalarFunction.builder(EnumCast.class, CAST)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice.implementation(
                        methodsGroup -> methodsGroup.methods("varcharToEnum").withExtraParameters(
                                context -> ImmutableList.of(enumType)))).build();
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
