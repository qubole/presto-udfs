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
package com.qubole.presto.udfs.scalar;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Created by stagra on 2/17/15.
 */
public class NumberSystemFunctions
{
    private NumberSystemFunctions() {}

    @Description("Converts decimal number to binary")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice bin(@SqlType(StandardTypes.BIGINT) long num)
    {
        String bin = Long.toBinaryString(num);
        return Slices.copiedBuffer(bin, UTF_8);
    }
}
