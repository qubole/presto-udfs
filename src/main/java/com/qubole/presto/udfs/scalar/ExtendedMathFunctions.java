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

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

/**
 * Created by stagra on 2/17/15.
 */
public class ExtendedMathFunctions
{
    private ExtendedMathFunctions() {}

    /*
     * These functions are now part of presto. Leaving the code here to serve as example to write scalar uds
     * */
    @Description("Converts radians to degrees")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double Qdegrees(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.toDegrees(num);
    }

    @Description("Converts degress to radians")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double Qradians(@SqlType(StandardTypes.DOUBLE) double num)
    {
        return Math.toRadians(num);
    }
}
