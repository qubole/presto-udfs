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
package com.facebook.presto.qubole.udfs.scalar.hiveudf;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.sql.Timestamp;
import java.time.MonthDay;
import java.time.Year;
import java.time.ZoneId;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static com.facebook.presto.operator.scalar.DateTimeFunctions.formatDatetime;
import static com.facebook.presto.operator.scalar.DateTimeFunctions.timeZoneHourFromTimestampWithTimeZone;
import static com.facebook.presto.operator.scalar.DateTimeFunctions.timeZoneMinuteFromTimestampWithTimeZone;
import static com.facebook.presto.operator.scalar.DateTimeFunctions.addFieldValueDate;
import static com.facebook.presto.operator.scalar.DateTimeFunctions.toISO8601FromDate;
import static com.facebook.presto.operator.scalar.DateTimeFunctions.fromISO8601Date;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.airlift.slice.Slices.utf8Slice;

/**
 * Created by apoorvg on 6/14/16.
 */
public class ExtendedDateTimeFunctions
{
    private ExtendedDateTimeFunctions() {}

    @Description("given timestamp in UTC and converts to given timezone")
    @ScalarFunction("from_utc_timestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long fromUtcTimestamp(@SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType(StandardTypes.VARCHAR) Slice inputZoneId)
    {
        ZoneId zoneId = ZoneId.of(inputZoneId.toStringUtf8(), ZoneId.SHORT_IDS);
        long offsetTimestamp = packDateTimeWithZone(timestamp, zoneId.toString());
        return  timestamp + ((timeZoneHourFromTimestampWithTimeZone(offsetTimestamp) * 60 + timeZoneMinuteFromTimestampWithTimeZone(offsetTimestamp)) * 60) * 1000;
    }

    @Description("given timestamp (in varchar) in UTC and converts to given timezone")
    @ScalarFunction("from_utc_timestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long fromUtcTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp, @SqlType(StandardTypes.VARCHAR) Slice inputZoneId)
    {
        Timestamp javaTimestamp = Timestamp.valueOf(inputTimestamp.toStringUtf8());
        ZoneId zoneId = ZoneId.of(inputZoneId.toStringUtf8(), ZoneId.SHORT_IDS);
        long offsetTimestamp = packDateTimeWithZone(javaTimestamp.getTime(), zoneId.toString());
        return  javaTimestamp.getTime() + ((timeZoneHourFromTimestampWithTimeZone(offsetTimestamp) * 60 + timeZoneMinuteFromTimestampWithTimeZone(offsetTimestamp)) * 60) * 1000;
    }

    @Description("given timestamp in a timezone convert it to UTC")
    @ScalarFunction("to_utc_timestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long toUtcTimestamp(@SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType(StandardTypes.VARCHAR) Slice inputZoneId)
    {
        ZoneId zoneId = ZoneId.of(inputZoneId.toStringUtf8(), ZoneId.SHORT_IDS);
        long offsetTimestamp =  packDateTimeWithZone(timestamp, zoneId.toString());
        return timestamp - ((timeZoneHourFromTimestampWithTimeZone(offsetTimestamp) * 60 + timeZoneMinuteFromTimestampWithTimeZone(offsetTimestamp)) * 60) * 1000;
    }

    @Description("given timestamp (in varchar) in a timezone convert it to UTC")
    @ScalarFunction("to_utc_timestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long toUtcTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp, @SqlType(StandardTypes.VARCHAR) Slice inputZoneId)
    {
        Timestamp javaTimestamp = Timestamp.valueOf(inputTimestamp.toStringUtf8());
        ZoneId zoneId = ZoneId.of(inputZoneId.toStringUtf8(), ZoneId.SHORT_IDS);
        long offsetTimestamp = packDateTimeWithZone(javaTimestamp.getTime(), zoneId.toString());
        return  javaTimestamp.getTime() - ((timeZoneHourFromTimestampWithTimeZone(offsetTimestamp) * 60 + timeZoneMinuteFromTimestampWithTimeZone(offsetTimestamp)) * 60) * 1000;
    }

    @Description("Returns the date part of the timestamp string")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice stringTimestampToDate(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]]");
        LocalDate date = LocalDate.parse(inputTimestamp.toStringUtf8(), formatter);
        return utf8Slice(date.toString());
    }

    @Description("Returns the date part of the timestamp")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice timestampToDate(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
          return formatDatetime(session, timestamp, Slices.utf8Slice("yyyy-MM-dd"));
    }

    @Description("Returns the date part of the timestamp with time zone")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice timestampWithTimeZoneToDate(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        // Right shifting 6 bits, converts a timestamp with timezone to timestamp.
        return formatDatetime(session, timestamp >> 6, Slices.utf8Slice("yyyy-MM-dd"));
    }

    @Description("Gets current UNIX timestamp in seconds")
    @ScalarFunction("unix_timestamp")
    @SqlType(StandardTypes.BIGINT)
    public static long currentUnixTimestamp(ConnectorSession session)
    {
        return session.getStartTime() / 1000;
    }

    @Description("Subtract number of days to the given date")
    @ScalarFunction("date_sub")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateSub(ConnectorSession session, @SqlType(StandardTypes.DATE) long date, @SqlType(StandardTypes.BIGINT) long value)
    {
        date = addFieldValueDate(session, Slices.utf8Slice("day"), -value, date);
        return toISO8601FromDate(session, date);
    }

    @Description("Subtract number of days to the given string date")
    @ScalarFunction("date_sub")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateSubString(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice inputDate, @SqlType(StandardTypes.BIGINT) long value)
    {
        long date = fromISO8601Date(session, inputDate);
        date = addFieldValueDate(session, Slices.utf8Slice("day"), -value, date);
        return toISO8601FromDate(session, date);
    }

    @Description("year of the given string timestamp")
    @ScalarFunction("year")
    @SqlType(StandardTypes.BIGINT)
    public static long yearFromStringTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]]");
        return Year.parse(inputTimestamp.toStringUtf8(), formatter).getValue();
    }

    @Description("month of the year of the given string timestamp")
    @ScalarFunction("month")
    @SqlType(StandardTypes.BIGINT)
    public static long monthFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]]");
        return MonthDay.parse(inputTimestamp.toStringUtf8(), formatter).getMonthValue();
    }

    @Description("day of the year of the given string timestamp")
    @ScalarFunction("day")
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]]");
        return MonthDay.parse(inputTimestamp.toStringUtf8(), formatter).getDayOfMonth();
    }
}
