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
package com.qubole.presto.udfs;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;
import com.qubole.presto.udfs.scalar.hiveUdfs.ExtendedDateTimeFunctions;
import com.qubole.presto.udfs.scalar.hiveUdfs.ExtendedMathematicFunctions;
import com.qubole.presto.udfs.scalar.hiveUdfs.ExtendedStringFunctions;
import com.qubole.presto.udfs.window.FirstNonNullValueFunction;
import com.qubole.presto.udfs.window.LastNonNullValueFunction;

import java.util.Set;

public class UdfPlugin implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        /*
         * Presto 0.157 does not expose the interfaces to add SqlFunction objects directly
         * We can only add udfs via Annotations now
         *
         * Unsupported udfs right now:
         * Hash
         * Nvl
         * array_aggr
         */
        return ImmutableSet.<Class<?>>builder()
                .add(ExtendedDateTimeFunctions.class)
                .add(ExtendedMathematicFunctions.class)
                .add(ExtendedStringFunctions.class)
                .add(FirstNonNullValueFunction.class)
                .add(LastNonNullValueFunction.class)
                .build();
    }
}
