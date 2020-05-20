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

import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.CallSiteBinder;
import io.prestosql.util.CompilerUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.stream.Collectors;

import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.sql.gen.BytecodeUtils.invoke;
import static io.prestosql.util.Reflection.methodHandle;
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
    public ScalarFunctionImplementation specialize(BoundVariables types, int arity, Metadata metadata)
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
        MethodHandle nvlMethodHandle = methodHandle(clazz, "nvl", stackTypes.toArray(new Class<?>[0]));

        return new ScalarFunctionImplementation(true, ImmutableList.of(valueTypeArgumentProperty(USE_BOXED_TYPE), valueTypeArgumentProperty(USE_BOXED_TYPE)), nvlMethodHandle, isDeterministic());
    }

    private Class<?> ifNull(List<Class<?>> nativeContainerTypes)
    {
        List<String> nativeContainerTypeNames = nativeContainerTypes.stream().map(Class::getSimpleName).collect(Collectors.toList());
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
        return value == null;
    }

    @UsedByGeneratedCode
    public static boolean checkNullB(Boolean value)
    {
        return value == null;
    }

    @UsedByGeneratedCode
    public static boolean checkNullD(Double value)
    {
        return value == null;
    }

    @UsedByGeneratedCode
    public static boolean checkNullS(Slice value)
    {
        return value == null;
    }
}
