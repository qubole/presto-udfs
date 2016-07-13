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

import java.util.Random;
import javax.annotation.Nullable;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;

/**
 * Created by apoorvg on 6/29/16.
 */
public class ExtendedMathematicFunctions
{
    private ExtendedMathematicFunctions() {}

    @Description("a seed pseudo-random value")
    @ScalarFunction("rands")
    @SqlType(StandardTypes.DOUBLE)
    public static double randomWithSeed(@SqlType(StandardTypes.BIGINT) long seed)
    {
        Random rand = new Random(seed);
        return rand.nextDouble();
    }

    @Nullable
    @Description("Returns the positive value of a mod b.")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.BIGINT)
    public static Long positiveModulus(@SqlType(StandardTypes.BIGINT) long a, @SqlType(StandardTypes.BIGINT) long b)
    {
        if (b == 0) {
            return null;
        }
        return java.lang.Math.floorMod(a, b);
    }

    @Nullable
    @Description("Returns the positive value of a mod b ")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.DOUBLE)
    public static Double positiveModulusDouble(@SqlType(StandardTypes.DOUBLE) double a, @SqlType(StandardTypes.DOUBLE) double b)
    {
        if (b == 0) {
            return null;
        }

        if ((a >= 0 && b > 0) || (a <= 0 && b < 0)) {
            return a % b;
        }
        else {
            return (a % b) + b;
        }
    }
}
