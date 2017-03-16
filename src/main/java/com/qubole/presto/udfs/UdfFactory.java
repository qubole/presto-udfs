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

import com.facebook.presto.metadata.FunctionListBuilder;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.WindowFunction;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by stagra on 2/17/15.
 */

// This class is not used anymore as 0.157 does not provide a way to directly add SqlFunctions
public class UdfFactory
{
    //private final TypeManager typeManager;
    private static final Logger log = Logger.get(UdfFactory.class);

    /*public UdfFactory(TypeManager tm)
    {
        this.typeManager = tm;
    }*/

    public Set<SqlFunction> listFunctions()
    {
        FunctionListBuilder builder = new FunctionListBuilder();
        try {
            List<Class<?>> classes = getFunctionClasses();
            addFunctions(builder, classes);
        }
        catch (IOException e) {
            System.out.println("Could not load classes from jar file: " + e);
            return Sets.newHashSet();
        }

        return Sets.newHashSet(builder.getFunctions());
    }

    private void addFunctions(FunctionListBuilder builder, List<Class<?>> classes)
    {
        for (Class<?> clazz : classes) {
            log.info("Adding: " + clazz);
            if (SqlAggregationFunction.class.isAssignableFrom(clazz)) {
                try {
                    builder.function((SqlAggregationFunction) clazz.newInstance());
                }
                catch (InstantiationException | IllegalAccessException e) {
                    log.info(String.format("Could not add %s, exception: %s, stack: %s", clazz.getName(), e, e.getStackTrace()));
                }
            }
            else {
                if (clazz.getName().startsWith("com.qubole.presto.udfs.scalar")) {
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
                else if (clazz.getName().startsWith("com.qubole.presto.udfs.aggregation")) {
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
                else if (clazz.getName().startsWith("com.qubole.presto.udfs.window")) {
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
                else if (clazz.getName().startsWith("com.qubole.presto.udfs.sqlFunction")) {
                    try {
                        builder.function((SqlFunction) clazz.newInstance());
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
