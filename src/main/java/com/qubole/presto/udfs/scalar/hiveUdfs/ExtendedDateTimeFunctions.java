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
package com.qubole.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.IsoFields;

import static com.qubole.presto.udfs.scalar.hiveUdfs.PrestoDateTimeFunctions.addFieldValueDate;
import static com.qubole.presto.udfs.scalar.hiveUdfs.PrestoDateTimeFunctions.diffDate;
import static com.qubole.presto.udfs.scalar.hiveUdfs.PrestoDateTimeFunctions.diffTimestamp;
import static com.qubole.presto.udfs.scalar.hiveUdfs.PrestoDateTimeFunctions.diffTimestampWithTimeZone;
import static com.qubole.presto.udfs.scalar.hiveUdfs.PrestoDateTimeFunctions.formatDatetime;
import static com.qubole.presto.udfs.scalar.hiveUdfs.PrestoDateTimeFunctions.timeZoneHourFromTimestampWithTimeZone;
import static com.qubole.presto.udfs.scalar.hiveUdfs.PrestoDateTimeFunctions.timeZoneMinuteFromTimestampWithTimeZone;
import static com.qubole.presto.udfs.scalar.hiveUdfs.PrestoDateTimeFunctions.toISO8601FromDate;
import static com.qubole.presto.udfs.scalar.hiveUdfs.PrestoDateTimeFunctions.toUnixTimeFromTimestampWithTimeZone;
import static com.qubole.presto.udfs.scalar.hiveUdfs.PrestoDateTimeFunctions.weekFromTimestamp;
import static com.qubole.presto.udfs.scalar.hiveUdfs.PrestoDateTimeFunctions.weekFromTimestampWithTimeZone;
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
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
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
        // Offset is added to the unix timestamp to incorporate the affect of Timezone.
        long offset = ((timeZoneHourFromTimestampWithTimeZone(timestamp) * 60 + timeZoneMinuteFromTimestampWithTimeZone(timestamp)) * 60) * 1000;
        return formatDatetime(session, ((long) toUnixTimeFromTimestampWithTimeZone(timestamp) * 1000) + offset, Slices.utf8Slice("yyyy-MM-dd"));
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
    public static Slice stringDateSub(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice inputDate, @SqlType(StandardTypes.BIGINT) long value)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
        LocalDate date = LocalDate.parse(inputDate.toStringUtf8(), formatter);
        date = LocalDate.ofEpochDay(date.toEpochDay() - value);
        return utf8Slice(date.toString());
    }

    @Description("Add number of days to the given date")
    @ScalarFunction("date_add")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateAdd(ConnectorSession session, @SqlType(StandardTypes.DATE) long date, @SqlType(StandardTypes.BIGINT) long value)
    {
        date = addFieldValueDate(session, Slices.utf8Slice("day"), value, date);
        return toISO8601FromDate(session, date);
    }

    @Description("Add number of days to the given string date")
    @ScalarFunction("date_add")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice StringDateAdd(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice inputDate, @SqlType(StandardTypes.BIGINT) long value)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
        LocalDate date = LocalDate.parse(inputDate.toStringUtf8(), formatter);
        date = LocalDate.ofEpochDay(date.toEpochDay() + value);
        return utf8Slice(date.toString());
    }

    @Description("year of the given string timestamp")
    @ScalarFunction("year")
    @SqlType(StandardTypes.BIGINT)
    public static long yearFromStringTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
        return Year.parse(inputTimestamp.toStringUtf8(), formatter).getValue();
    }

    @Description("month of the year of the given string timestamp")
    @ScalarFunction("month")
    @SqlType(StandardTypes.BIGINT)
    public static long monthFromStringTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
        return MonthDay.parse(inputTimestamp.toStringUtf8(), formatter).getMonthValue();
    }

    @Description("week of the year of the given string timestamp")
    @ScalarFunction("weekofyear")
    @SqlType(StandardTypes.BIGINT)
    public static long weekOfYearFromStringTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
        LocalDate date = LocalDate.parse(inputTimestamp.toStringUtf8(), formatter);
        return date.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
    }

    @Description("week of the year of the given string timestamp")
    @ScalarFunction("weekofyear")
    @SqlType(StandardTypes.BIGINT)
    public static long weekOfYearFromTimestamp(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return weekFromTimestamp(session, timestamp);
    }

    @Description("week of the year of the given string timestamp")
    @ScalarFunction("weekofyear")
    @SqlType(StandardTypes.BIGINT)
    public static long weekOfYearFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return weekFromTimestampWithTimeZone(timestamp);
    }

    @Description("day of the year of the given string timestamp")
    @ScalarFunction(value = "day", alias = "dayofmonth")
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
        return MonthDay.parse(inputTimestamp.toStringUtf8(), formatter).getDayOfMonth();
    }

    @Description("day of the year of the given string timestamp")
    @ScalarFunction("hour")
    @SqlType(StandardTypes.BIGINT)
    public static long hourFromTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("[yyyy-MM-dd ]HH:mm:ss[.SSS][ zzz]");
        return LocalTime.parse(inputTimestamp.toStringUtf8(), formatter).getHour();
    }

    @Description("day of the year of the given string timestamp")
    @ScalarFunction("minute")
    @SqlType(StandardTypes.BIGINT)
    public static long minuteFromTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("[yyyy-MM-dd ]HH:mm:ss[.SSS][ zzz]");
        return LocalTime.parse(inputTimestamp.toStringUtf8(), formatter).getMinute();
    }

    @Description("day of the year of the given string timestamp")
    @ScalarFunction("second")
    @SqlType(StandardTypes.BIGINT)
    public static long secondFromTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("[yyyy-MM-dd ]HH:mm:ss[.SSS][ zzz]");
        return LocalTime.parse(inputTimestamp.toStringUtf8(), formatter).getSecond();
    }

    @Description("difference of the given dates (String) in days")
    @ScalarFunction("datediff")
    @SqlType(StandardTypes.BIGINT)
    public static long diffStringDateInDays(@SqlType(StandardTypes.VARCHAR) Slice inputDate1, @SqlType(StandardTypes.VARCHAR) Slice inputDate2)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
        LocalDate date1 = LocalDate.parse(inputDate1.toStringUtf8(), formatter);
        LocalDate date2 = LocalDate.parse(inputDate2.toStringUtf8(), formatter);
        return date1.toEpochDay() - date2.toEpochDay();
    }

    @Description("difference of the given dates in days")
    @ScalarFunction("datediff")
    @SqlType(StandardTypes.BIGINT)
    public static long diffDateInDays(ConnectorSession session, @SqlType(StandardTypes.DATE) long date1, @SqlType(StandardTypes.DATE) long date2)
    {
        return diffDate(session, utf8Slice("day"), date2, date1);
    }

    @Description("difference of the given dates (Timestamps) in days")
    @ScalarFunction("datediff")
    @SqlType(StandardTypes.BIGINT)
    public static long diffTimestampDateInDays(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long timestamp1, @SqlType(StandardTypes.TIMESTAMP) long timestamp2)
    {
        return diffTimestamp(session, utf8Slice("day"), timestamp2, timestamp1);
    }

    @Description("difference of the given dates (Timestamps) in days")
    @ScalarFunction("datediff")
    @SqlType(StandardTypes.BIGINT)
    public static long diffTimestampWithTimezoneDateInDays(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp1, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp2)
    {
        return diffTimestampWithTimeZone(utf8Slice("day"), timestamp2, timestamp1);
    }

    @Description("Converts the number of seconds from unix epoch to a string representing the timestamp")
    @ScalarFunction("format_unixtimestamp")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUnixtimeToStringTimestamp(@SqlType(StandardTypes.BIGINT) long epochtime)
    {
        LocalDateTime timestamp = LocalDateTime.ofEpochSecond(epochtime, 0, ZoneOffset.UTC);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return utf8Slice(timestamp.format(formatter));
    }

    @Description("Converts the number of seconds from unix epoch to a string representing the timestamp according to the given format")
    @ScalarFunction("format_unixtimestamp")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUnixtimeWithFormatToStringTimestamp(@SqlType(StandardTypes.BIGINT) long epochtime, @SqlType(StandardTypes.VARCHAR) Slice format)
    {
        ZonedDateTime timestamp = ZonedDateTime.of(LocalDateTime.ofEpochSecond(epochtime, 0, ZoneOffset.UTC), ZoneId.of("UTC"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format.toStringUtf8());
        return utf8Slice(timestamp.format(formatter));
    }
}
