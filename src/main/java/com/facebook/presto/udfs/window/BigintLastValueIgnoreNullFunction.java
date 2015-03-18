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
package com.facebook.presto.udfs.window;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.udfs.WindowFunctionDefinition;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

/**
 * Created by user on 13-03-2015.
 */
public class BigintLastValueIgnoreNullFunction
        extends FirstOrLastValueIgnoreNullFunction
        implements WindowFunctionDefinition
{
    public BigintLastValueIgnoreNullFunction(List<Integer> argumentChannels)
    {
        super(BIGINT, argumentChannels, Direction.LAST);
    }

    public BigintLastValueIgnoreNullFunction()
    {
        // Constructor is used by UdfFactory
        super();
    }

    @Override
    public String getName()
    {
        return "last_non_null_value";
    }

    @Override
    public Type getReturnType()
    {
        return BIGINT;
    }

    @Override
    public List<? extends Type> getArgumentTypes()
    {
        return ImmutableList.<Type>of(BIGINT);
    }
}
