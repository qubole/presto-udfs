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

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.operator.scalar.StringFunctions.stringPosition;
import static com.facebook.presto.operator.scalar.StringFunctions.substr;
import static io.airlift.slice.SliceUtf8.countCodePoints;

/**
 * Created by apoorvg on 28/06/16.
 */
public class ExtendedStringFunctions
{
    private ExtendedStringFunctions() {}

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("locate")
    @SqlType(StandardTypes.BIGINT)
    public static long locateString(@SqlType(StandardTypes.VARCHAR) Slice substring, @SqlType(StandardTypes.VARCHAR) Slice string)
    {
        if (substring.length() == 0) {
            return 1;
        }
        if (string.length() == 0) {
            return 0;
        }
        return stringPosition(string, substring);
    }

    @Description("Returns the position of the first occurrence of substring in str after position pos")
    @ScalarFunction("locate")
    @SqlType(StandardTypes.BIGINT)
    public static long locateStringWithPos(@SqlType(StandardTypes.VARCHAR) Slice substring, @SqlType(StandardTypes.VARCHAR) Slice inputString, @SqlType(StandardTypes.BIGINT) long pos)
    {
        if (substring.length() == 0) {
            if (inputString.length() + 1 >= pos) {
               return 1;
            }
            else {
                return 0;
            }
        }
        if (inputString.length() == 0 || inputString.length() < pos) {
            return 0;
        }
        Slice string = substr(inputString, pos);
        int index = string.indexOf(substring);
        if (index < 0) {
            return 0;
        }
        return pos + countCodePoints(string, 0, index);
    }

    @Description("Returns the first occurance of string in string list (inputStrList) where string list (inputStrList) is a comma-delimited string.")
    @ScalarFunction("find_in_set")
    @SqlType(StandardTypes.BIGINT)
    public static long findInSet(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice inputStrList)
    {
        if (string.length() == 0 && inputStrList.length() == 0) {
            return 1;
        }
        String[] strList = (inputStrList.toStringUtf8()).split(",");
        long pos = 1;
        for (String s : strList) {
            if (s.equals(string.toStringUtf8())) {
                return pos;
            }
            pos++;
        }
        return 0;
    }
}
