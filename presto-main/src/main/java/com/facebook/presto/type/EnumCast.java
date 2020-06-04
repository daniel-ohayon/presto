package com.facebook.presto.type;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.type.NumericEnumType.MOOD_ENUM;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;

public final class EnumCast
{
    private EnumCast() {}

    @ScalarOperator(CAST)
    @SqlType("Mood")
    public static long castToMood(@SqlType(StandardTypes.VARCHAR) Slice value)
    {
        final Long numericValue = MOOD_ENUM.entries.get(value.toStringUtf8());
        if (numericValue == null) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Invalid enum value");
        }
        return numericValue;
    }

    @ScalarOperator(CAST)
    @SqlType("Mood")
    public static long castToMood(@SqlType(StandardTypes.INTEGER) long value)
    {
        if (!MOOD_ENUM.entries.values().contains(value)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Invalid enum value");
        }
        return value;
    }
}
