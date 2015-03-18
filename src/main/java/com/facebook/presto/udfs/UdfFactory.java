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

package com.facebook.presto.udfs;

import com.facebook.presto.metadata.FunctionFactory;
import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.metadata.ParametricAggregation;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import io.airlift.log.Logger;

/**
 * Created by stagra on 2/17/15.
 */
public class UdfFactory implements FunctionFactory
{
    private final TypeManager typeManager;
    private static final Logger log = Logger.get(UdfFactory.class);

    public UdfFactory(TypeManager tm)
    {
        this.typeManager = tm;
    }
    @Override
    public List<ParametricFunction> listFunctions()
    {
        FunctionListBuilder builder = new FunctionListBuilder(typeManager);
        try {
            List<Class<?>> classes = getFunctionClasses();
            addFunctions(builder, classes);
        }
        catch (IOException e) {
            System.out.println("Could not load classes from jar file: " + e);
            return ImmutableList.of();
        }

        return builder.getFunctions();
    }

    private void addFunctions(FunctionListBuilder builder, List<Class<?>> classes)
    {
        for (Class<?> clazz : classes) {
            log.info("Adding: " + clazz);
            if (ParametricAggregation.class.isAssignableFrom(clazz)) {
                try {
                    builder.function((ParametricAggregation) clazz.newInstance());
                }
                catch (InstantiationException | IllegalAccessException e) {
                    log.info(String.format("Could not add %s, exception: %s, stack: %s", clazz.getName(), e, e.getStackTrace()));
                }
            }
            else {
                if (clazz.getName().startsWith("com.facebook.presto.udfs.scalar")) {
                    try {
                        builder.scalar(clazz);
                    }
                    catch (Exception e) {
                        if (e.getCause() instanceof IllegalAccessException) {
                            // This is alright, must be helper classes
                        }
                        else {
                            log.info(String.format("Could not add %s, exception: %s, stack: %s", clazz.getName(), e, e.getStackTrace()));
                        }
                    }
                }
                else if (clazz.getName().startsWith("com.facebook.presto.udfs.aggregation")) {
                    AggregationFunction aggregationAnnotation = clazz.getAnnotation(AggregationFunction.class);
                    if (aggregationAnnotation == null) {
                        continue;
                    }
                    try {
                        builder.aggregate(clazz);
                    }
                    catch (Exception e) {
                        log.info(String.format("Could not add %s, exception: %s, stack: %s", clazz.getName(), e, e.getStackTrace()));
                    }
                }
                else if (clazz.getName().startsWith("com.facebook.presto.udfs.window")) {
                    if (WindowFunctionDefinition.class.isAssignableFrom(clazz)) {
                        try {
                            WindowFunctionDefinition def = (WindowFunctionDefinition) clazz.newInstance();
                            builder.window(def.getName(), def.getReturnType(), def.getArgumentTypes(), (Class<? extends WindowFunction>) clazz);
                        }
                        catch (InstantiationException | IllegalAccessException e) {
                            log.info(String.format("Could not add %s, exception: %s, stack: %s", clazz.getName(), e, e.getStackTrace()));
                        }
                    }

                }

            }
        }
    }

    private List<Class<?>> getFunctionClasses()
            throws IOException
    {
        List<Class<?>> classes = new ArrayList<Class<?>>();
        String classResource = this.getClass().getName().replace(".", "/") + ".class";
        String jarURLFile = Thread.currentThread().getContextClassLoader().getResource(classResource).getFile();
        int jarEnd = jarURLFile.indexOf('!');
        String jarLocation = jarURLFile.substring(0, jarEnd); // This is in URL format, convert once more to get actual file location
        jarLocation = new URL(jarLocation).getFile();

        List<String> classNames = new ArrayList<String>();
        ZipInputStream zip = new ZipInputStream(new FileInputStream(jarLocation));
        for (ZipEntry entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
            if (entry.getName().endsWith(".class") && !entry.isDirectory()) {
                String className = entry.getName().replace("/", "."); // This still has .class at the end
                className = className.substring(0, className.length() - 6); // remvove .class from end
                try {
                    classes.add(Class.forName(className));
                }
                catch (ClassNotFoundException e) {
                    System.out.println(String.format("Could not load class %s, Exception: %s", className, e));
                    //TODO: add log
                }
            }
        }
        return classes;
    }
}
