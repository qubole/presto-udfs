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
package com.qubole.presto.udfs.window;

import com.facebook.presto.spi.function.WindowFunctionSignature;

import java.util.List;

/**
 * Created by shubham on 14/03/17.
 */
@WindowFunctionSignature(name = "first_non_null_value", typeVariable = "T", returnType = "T", argumentTypes = "T")
public class FirstNonNullValueFunction extends FirstOrLastValueIgnoreNullFunction
{
    public FirstNonNullValueFunction(List<Integer> argumentChannels)
    {
        super(argumentChannels, Direction.FIRST);
    }
}
