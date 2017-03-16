/*
 * Copyright 2013-2016 Qubole
 *
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
package com.qubole.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import org.joda.time.Chronology;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DurationField;
import org.joda.time.DurationFieldType;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.field.DividedDateTimeField;
import org.joda.time.field.OffsetDateTimeField;
import org.joda.time.field.ScaledDurationField;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Locale;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.qubole.presto.udfs.scalar.hiveUdfs.PrestoDateTimeZoneIndex.extractZoneOffsetMinutes;
import static com.qubole.presto.udfs.scalar.hiveUdfs.PrestoDateTimeZoneIndex.getChronology;
import static com.qubole.presto.udfs.scalar.hiveUdfs.PrestoDateTimeZoneIndex.unpackChronology;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.joda.time.DateTimeZone.UTC;

// This is copy of DateTimeFunctions because presto does not provide presto-main jars to plugins anymore
public final class PrestoDateTimeFunctions
{
    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstance(UTC);
    private static final int MILLISECONDS_IN_SECOND = 1000;
    private static final int MILLISECONDS_IN_MINUTE = 60 * MILLISECONDS_IN_SECOND;
    private static final int MILLISECONDS_IN_HOUR = 60 * MILLISECONDS_IN_MINUTE;

    public static final DateTimeFieldType QUARTER_OF_YEAR = new QuarterOfYearDateTimeField();

    private PrestoDateTimeFunctions() {}

    public static double toUnixTimeFromTimestampWithTimeZone(long timestampWithTimeZone)
    {
        return unpackMillisUtc(timestampWithTimeZone) / 1000.0;
    }

    public static Slice toISO8601FromDate(ConnectorSession session,  long date)
    {
        DateTimeFormatter formatter = ISODateTimeFormat.date()
                .withChronology(UTC_CHRONOLOGY);
        return utf8Slice(formatter.print(DAYS.toMillis(date)));
    }

    public static long addFieldValueDate(ConnectorSession session,  Slice unit,  long value,  long date)
    {
        long millis = getDateField(UTC_CHRONOLOGY, unit).add(DAYS.toMillis(date), Ints.checkedCast(value));
        return MILLISECONDS.toDays(millis);
    }

    public static long diffDate(ConnectorSession session, Slice unit,  long date1,  long date2)
    {
        return getDateField(UTC_CHRONOLOGY, unit).getDifferenceAsLong(DAYS.toMillis(date2), DAYS.toMillis(date1));
    }

    public static long diffTimestamp(
            ConnectorSession session,
            Slice unit,
            long timestamp1,
            long timestamp2)
    {
        return getTimestampField(getChronology(session.getTimeZoneKey()), unit).getDifferenceAsLong(timestamp2, timestamp1);
    }

    public static long diffTimestampWithTimeZone(
            Slice unit,
            long timestampWithTimeZone1,
            long timestampWithTimeZone2)
    {
        return getTimestampField(unpackChronology(timestampWithTimeZone1), unit).getDifferenceAsLong(unpackMillisUtc(timestampWithTimeZone2), unpackMillisUtc(timestampWithTimeZone1));
    }

    private static DateTimeField getDateField(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        switch (unitString) {
            case "day":
                return chronology.dayOfMonth();
            case "week":
                return chronology.weekOfWeekyear();
            case "month":
                return chronology.monthOfYear();
            case "quarter":
                return QUARTER_OF_YEAR.getField(chronology);
            case "year":
                return chronology.year();
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid DATE field");
    }

    private static DateTimeField getTimestampField(ISOChronology chronology, Slice unit)
    {
        String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
        switch (unitString) {
            case "millisecond":
                return chronology.millisOfSecond();
            case "second":
                return chronology.secondOfMinute();
            case "minute":
                return chronology.minuteOfHour();
            case "hour":
                return chronology.hourOfDay();
            case "day":
                return chronology.dayOfMonth();
            case "week":
                return chronology.weekOfWeekyear();
            case "month":
                return chronology.monthOfYear();
            case "quarter":
                return QUARTER_OF_YEAR.getField(chronology);
            case "year":
                return chronology.year();
        }
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid Timestamp field");
    }

    public static Slice formatDatetime(ConnectorSession session, long timestamp, Slice formatString)
    {
        return formatDatetime(getChronology(session.getTimeZoneKey()), session.getLocale(), timestamp, formatString);
    }

    private static Slice formatDatetime(ISOChronology chronology, Locale locale, long timestamp, Slice formatString)
    {
        try {
            return utf8Slice(DateTimeFormat.forPattern(formatString.toStringUtf8())
                    .withChronology(chronology)
                    .withLocale(locale)
                    .print(timestamp));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    public static long weekFromTimestamp(ConnectorSession session, long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).weekOfWeekyear().get(timestamp);
    }

    public static long weekFromTimestampWithTimeZone(long timestampWithTimeZone)
    {
        return unpackChronology(timestampWithTimeZone).weekOfWeekyear().get(unpackMillisUtc(timestampWithTimeZone));
    }

    public static long timeZoneMinuteFromTimestampWithTimeZone(long timestampWithTimeZone)
    {
        return extractZoneOffsetMinutes(timestampWithTimeZone) % 60;
    }

    public static long timeZoneHourFromTimestampWithTimeZone(long timestampWithTimeZone)
    {
        return extractZoneOffsetMinutes(timestampWithTimeZone) / 60;
    }

    private static class QuarterOfYearDateTimeField
            extends DateTimeFieldType
    {
        private static final long serialVersionUID = -5677872459807379123L;

        private static final DurationFieldType QUARTER_OF_YEAR_DURATION_FIELD_TYPE = new QuarterOfYearDurationFieldType();

        private QuarterOfYearDateTimeField()
        {
            super("quarterOfYear");
        }

        @Override
        public DurationFieldType getDurationType()
        {
            return QUARTER_OF_YEAR_DURATION_FIELD_TYPE;
        }

        @Override
        public DurationFieldType getRangeDurationType()
        {
            return DurationFieldType.years();
        }

        @Override
        public DateTimeField getField(Chronology chronology)
        {
            return new OffsetDateTimeField(new DividedDateTimeField(new OffsetDateTimeField(chronology.monthOfYear(), -1), QUARTER_OF_YEAR, 3), 1);
        }

        private static class QuarterOfYearDurationFieldType
                extends DurationFieldType
        {
            private static final long serialVersionUID = -8167713675442491871L;

            public QuarterOfYearDurationFieldType()
            {
                super("quarters");
            }

            @Override
            public DurationField getField(Chronology chronology)
            {
                return new ScaledDurationField(chronology.months(), QUARTER_OF_YEAR_DURATION_FIELD_TYPE, 3);
            }
        }
    }
}
