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
package com.qubole.presto.udfs.sqlFunction.hiveUdfs;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.CompilerUtils;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.facebook.presto.util.ImmutableCollectors;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.String.format;

/**
 * Created by apoorvg on 7/24/16.
 */
public final class Nvl
       extends SqlScalarFunction
    {
    public static final Nvl nvl = new Nvl();
    private static final String NAME = "nvl";
    private static final MethodHandle CHECK_NULL_L = methodHandle(Nvl.class, "checkNullL", Long.class);
    private static final MethodHandle CHECK_NULL_B = methodHandle(Nvl.class, "checkNullB", Boolean.class);
    private static final MethodHandle CHECK_NULL_D = methodHandle(Nvl.class, "checkNullD", Double.class);
    private static final MethodHandle CHECK_NULL_S = methodHandle(Nvl.class, "checkNullS", Slice.class);

    public Nvl()
    {
        super(new Signature(
                NAME,
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("E")),
                ImmutableList.of(),
                parseTypeSignature("E"),
                ImmutableList.of(parseTypeSignature("E")),
                true));
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Returns default value if value is null else returns value ";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        if (arity != 2) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "There must be two arguments");
        }
        Type type = types.getTypeVariable("E");
        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        if (type.getJavaType() == long.class) {
            builder.add(Long.class);
            builder.add(Long.class);
        }
        else if (type.getJavaType() == double.class) {
            builder.add(Double.class);
            builder.add(Double.class);
        }
        else if (type.getJavaType() == boolean.class) {
            builder.add(Boolean.class);
            builder.add(Boolean.class);
        }
        else {
            builder.add(type.getJavaType());
            builder.add(type.getJavaType());
        }

        ImmutableList<Class<?>> stackTypes = builder.build();
        Class<?> clazz = ifNull(stackTypes);
        MethodHandle nvlMethodHandle = methodHandle(clazz, "nvl", stackTypes.toArray(new Class<?>[stackTypes.size()]));

        return new ScalarFunctionImplementation(true, ImmutableList.of(true, true), nvlMethodHandle, isDeterministic());
    }

    private Class<?> ifNull(List<Class<?>> nativeContainerTypes)
    {
        List<String> nativeContainerTypeNames = nativeContainerTypes.stream().map(Class::getSimpleName).collect(ImmutableCollectors.toImmutableList());
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(Joiner.on("").join(nativeContainerTypeNames) + "Nvl"),
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        for (int i = 0; i < nativeContainerTypes.size(); i++) {
            Class<?> nativeContainerType = nativeContainerTypes.get(i);
            parameters.add(arg("arg" + i, nativeContainerType));
        }

        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "nvl", type(nativeContainerTypes.get(1)), parameters.build());
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        CallSiteBinder binder = new CallSiteBinder();

        Class<?> nativeContainerType = nativeContainerTypes.get(0);

        BytecodeBlock trueBlock = new BytecodeBlock()
                .getVariable(scope.getVariable("arg1"))
                .ret(nativeContainerTypes.get(1));

        BytecodeBlock falseBlock = new BytecodeBlock()
                .getVariable(scope.getVariable("arg0"))
                .ret(nativeContainerTypes.get(0));

        BytecodeBlock conditionBlock = new BytecodeBlock();
         if (nativeContainerType == Long.class) {
             conditionBlock.comment("checkNull")
                     .append(scope.getVariable("arg0"))
                     .append(invoke(binder.bind(CHECK_NULL_L), "checkNullL"));
         }
         else if (nativeContainerType == Boolean.class) {
             conditionBlock.comment("checkNull")
                     .append(scope.getVariable("arg0"))
                     .append(invoke(binder.bind(CHECK_NULL_B), "checkNullB"));
         }
         else if (nativeContainerType == Double.class) {
             conditionBlock.comment("checkNull")
                     .append(scope.getVariable("arg0"))
                     .append(invoke(binder.bind(CHECK_NULL_D), "checkNullD"));
         }
         else if (nativeContainerType == Slice.class) {
             conditionBlock.comment("checkNull")
                     .append(scope.getVariable("arg0"))
                     .append(invoke(binder.bind(CHECK_NULL_S), "checkNullS"));
         }
         else {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Unexpected type %s", nativeContainerType.getName()));
         }

        body.append(new IfStatement()
                .condition(conditionBlock)
                .ifTrue(trueBlock)
                .ifFalse(falseBlock));

        return defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(Nvl.class.getClassLoader()));
    }

    @UsedByGeneratedCode
    public static boolean checkNullL(Long value)
    {
        if (value == null) {
            return true;
        }
        else {
            return false;
        }
    }

    @UsedByGeneratedCode
    public static boolean checkNullB(Boolean value)
    {
        if (value == null) {
            return true;
        }
        else {
            return false;
        }
    }

    @UsedByGeneratedCode
    public static boolean checkNullD(Double value)
    {
        if (value == null) {
            return true;
        }
        else {
            return false;
        }
    }

    @UsedByGeneratedCode
    public static boolean checkNullS(Slice value)
    {
        if (value == null) {
            return true;
        }
        else {
            return false;
        }
    }
}
