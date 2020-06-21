package com.facebook.presto.type;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.EnumType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.IntegerEnumType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.StringEnumType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.SignatureBuilder;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Signature;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;

public final class EnumOperators
{
    private EnumOperators() {}

    private static final String STRING_ENUM_KEY_LOOKUP_METHOD = "stringEnumKeyLookup";
    private static final String STRING_ENUM_VALUE_LOOKUP_METHOD = "stringEnumValueLookup";
    private static final String INTEGER_ENUM_KEY_LOOKUP_METHOD = "integerEnumKeyLookup";
    private static final String INTEGER_ENUM_VALUE_LOOKUP_METHOD = "integerEnumValueLookup";

    // TODO add cast functions from enum to base types too?

    public static Collection<SqlScalarFunction> makeEnumCastFunctions(EnumType enumType)
    {
        if (enumType instanceof IntegerEnumType) {
            Collection<SqlScalarFunction> casts = Stream.of(
                    TinyintType.TINYINT, SmallintType.SMALLINT, IntegerType.INTEGER, BigintType.BIGINT)
                    .map(intType -> makeCastFunction(intType.getTypeSignature(), enumType, INTEGER_ENUM_VALUE_LOOKUP_METHOD))
                    .collect(Collectors.toCollection(ArrayList::new));
            casts.add(makeCastFunction(VarcharType.VARCHAR.getTypeSignature(), enumType, INTEGER_ENUM_KEY_LOOKUP_METHOD));
            return casts;
        }
        else if (enumType instanceof StringEnumType) {
            return ImmutableList.of(
                    // TODO find a way to resolve the ambiguity between these two
                    makeCastFunction(VarcharType.VARCHAR.getTypeSignature(), enumType, STRING_ENUM_KEY_LOOKUP_METHOD),
                    makeCastFunction(VarcharType.VARCHAR.getTypeSignature(), enumType, STRING_ENUM_VALUE_LOOKUP_METHOD)
            );
        }
        else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, String.format("Unexpected enum type: %s", enumType.getClass().getSimpleName()));
        }
    }

    private static SqlScalarFunction makeCastFunction(TypeSignature fromType, EnumType enumType, String castMethodName)
    {
        Signature signature = SignatureBuilder.builder()
                .kind(SCALAR)
                .operatorType(CAST)
                .argumentTypes(fromType)
                .returnType(enumType.getTypeSignature())
                .build();
        return SqlScalarFunction.builder(EnumOperators.class, CAST)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice.implementation(
                        methodsGroup -> methodsGroup.methods(castMethodName)
                                .withExtraParameters(
                                        context -> ImmutableList.of(enumType))))
                .build();
    }

    @UsedByGeneratedCode
    public static Slice stringEnumKeyLookup(Slice value, StringEnumType enumType)
    {
        final String enumValue = enumType.getEntries().get(value.toStringUtf8());
        if (enumValue == null) {
            throw new PrestoException(
                    INVALID_CAST_ARGUMENT,
                    String.format(
                            "No key '%s' in enum '%s'",
                            value.toStringUtf8(),
                            enumType.getTypeSignature().getBase()));
        }
        return Slices.utf8Slice(enumValue);
    }

    @UsedByGeneratedCode
    public static Slice stringEnumValueLookup(Slice value, StringEnumType enumType)
    {
        if (!enumType.getEntries().values().contains(value.toStringUtf8())) {
            throw new PrestoException(INVALID_CAST_ARGUMENT,
                    String.format(
                            "No value '%s' in enum '%s'",
                            value.toStringUtf8(),
                            enumType.getTypeSignature().getBase()));
        }
        return Slices.copyOf(value);
    }

    @UsedByGeneratedCode
    public static long integerEnumKeyLookup(Slice value, IntegerEnumType enumType)
    {
        final Long enumValue = enumType.getEntries().get(value.toStringUtf8());
        if (enumValue == null) {
            throw new PrestoException(
                    INVALID_CAST_ARGUMENT,
                    String.format(
                            "No key '%s' in enum '%s'",
                            value.toStringUtf8(),
                            enumType.getTypeSignature().getBase()));
        }
        return enumValue;
    }

    @UsedByGeneratedCode
    public static long integerEnumValueLookup(long value, IntegerEnumType enumType)
    {
        if (!enumType.getEntries().values().contains(value)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT,
                    String.format(
                            "No value '%d' in enum '%s'",
                            value,
                            enumType.getTypeSignature().getBase()));
        }
        return value;
    }
}