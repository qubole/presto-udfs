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

import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.CallSiteBinder;
import io.prestosql.type.BigintOperators;
import io.prestosql.util.CompilerUtils;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.USE_BOXED_TYPE;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.prestosql.util.Reflection.methodHandle;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static java.lang.String.format;

public final class Hash
        extends SqlScalarFunction
{
    public static final Hash hash = new Hash();
    private static final String NAME = "hash";
    public Hash()
    {
        super(new Signature(
                NAME,
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("E")),
                ImmutableList.of(),
                parseTypeSignature("bigint"),
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
        return "get the hash value for variable no. of arguments of any type";
    }

    public static void checkNotNaN(double value)
    {
        if (Double.isNaN(value)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid argument to hash(): NaN");
        }
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables types, int arity, Metadata metadata)
    {
        Type type = types.getTypeVariable("E");

        // the argument need not be orderable, so no orderable check
        ImmutableList.Builder<Class<?>> builder = ImmutableList.builder();
        for (int i = 0; i < arity; i++) {
            builder.add(type.getJavaType());
        }

        ImmutableList<Class<?>> stackTypes = builder.build();
        Class<?> clazz = generateHash(stackTypes, type);
        MethodHandle methodHandle = methodHandle(clazz, "hash", stackTypes.toArray(new Class<?>[0]));
        List<Boolean> nullableParameters = ImmutableList.copyOf(Collections.nCopies(stackTypes.size(), false));

        List<ScalarFunctionImplementation.ArgumentProperty> argumentProperties = nullableParameters.stream()
                .map(nullable -> nullable
                        ? valueTypeArgumentProperty(USE_BOXED_TYPE)
                        : valueTypeArgumentProperty(RETURN_NULL_ON_NULL))
                .collect(Collectors.toList());
        return new ScalarFunctionImplementation(false, argumentProperties, methodHandle, isDeterministic());
    }

    public static Class<?> generateHash(List<Class<?>> nativeContainerTypes, Type type)
    {
        List<String> nativeContainerTypeNames = ImmutableList.copyOf(nativeContainerTypes.stream()
            .map(Class::getSimpleName)
            .collect(Collectors.toList()));
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(Joiner.on("").join(nativeContainerTypeNames) + "Hash"),
                type(Object.class));

        definition.declareDefaultConstructor(a(PRIVATE));

        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        for (int i = 0; i < nativeContainerTypes.size(); i++) {
            Class<?> nativeContainerType = nativeContainerTypes.get(i);
            parameters.add(arg("arg" + i, nativeContainerType));
        }

        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "hash", type(long.class), parameters.build());
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable typeVariable = scope.declareVariable(Type.class, "typeVariable");
        CallSiteBinder binder = new CallSiteBinder();

        body.comment("typeVariable = type;")
                .append(constantType(binder, type))
                .putVariable(typeVariable);

        for (int i = 0; i < nativeContainerTypes.size(); i++) {
            Class<?> nativeContainerType = nativeContainerTypes.get(i);
            Variable currentBlock = scope.declareVariable(io.prestosql.spi.block.Block.class, "block" + i);
            Variable blockBuilder = scope.declareVariable(BlockBuilder.class, "blockBuilder" + i);
            BytecodeBlock buildBlock = new BytecodeBlock()
                    .comment("blockBuilder%d = typeVariable.createBlockBuilder(new BlockBuilderStatus(),1, 32);", i)
                    .getVariable(typeVariable)
                    .newObject(BlockBuilderStatus.class)
                    .dup()
                    .invokeConstructor(BlockBuilderStatus.class)
                    .push(1)
                    .push(32)
                    .invokeInterface(Type.class, "createBlockBuilder", BlockBuilder.class, BlockBuilderStatus.class, int.class, int.class)
                    .putVariable(blockBuilder);

            String writeMethodName;
            if (nativeContainerType == long.class) {
                writeMethodName = "writeLong";
            }
            else if (nativeContainerType == boolean.class) {
                writeMethodName = "writeBoolean";
            }
            else if (nativeContainerType == double.class) {
                writeMethodName = "writeDouble";
            }
            else if (nativeContainerType == Slice.class) {
                writeMethodName = "writeSlice";
            }
            else {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Unexpected type %s", nativeContainerType.getName()));
            }

            if (type.getTypeSignature().getBase().equals(StandardTypes.DOUBLE)) {
                buildBlock.comment("arg0 != NaN")
                        .append(scope.getVariable("arg" + i))
                        .invokeStatic(Hash.class, "checkNotNaN", void.class, double.class);
            }

            BytecodeBlock writeBlock = new BytecodeBlock()
                    .comment("typeVariable.%s(blockBuilder%d, arg%d);", writeMethodName, i, i)
                    .getVariable(typeVariable)
                    .getVariable(blockBuilder)
                    .append(scope.getVariable("arg" + i))
                    .invokeInterface(Type.class, writeMethodName, void.class, BlockBuilder.class, nativeContainerType);

            buildBlock.append(writeBlock);

            BytecodeBlock storeBlock = new BytecodeBlock()
                    .comment("block%d = blockBuilder%d.build();", i, i)
                    .getVariable(blockBuilder)
                    .invokeInterface(BlockBuilder.class, "build", io.prestosql.spi.block.Block.class)
                    .putVariable(currentBlock);
            buildBlock.append(storeBlock);
            body.append(buildBlock);
        }
        Variable rangeVariable = scope.declareVariable(long.class, "range");
        body.comment("range = Integer.MAX_VALUE")
                .push(Integer.MAX_VALUE)
                .intToLong()
                .putVariable(rangeVariable);

        Variable hashValueVariable = scope.declareVariable(long.class, "hashValue");
        body.comment("hashValue = 0")
                .push(0)
                .intToLong()
                .putVariable(hashValueVariable);

        Variable currenHashValueVariable = scope.declareVariable(long.class, "currentHashValue");
        Variable currentBlockLengthVariable = scope.declareVariable(int.class, "currentLength");
        for (int i = 0; i < nativeContainerTypes.size(); i++) {
            BytecodeBlock currentBlockLength = new BytecodeBlock()
                    .append(scope.getVariable("block" + i))
                    .push(0)
                    .invokeInterface(io.prestosql.spi.block.Block.class, "getLength", int.class, int.class)
                    .putVariable(currentBlockLengthVariable);

            BytecodeBlock currentHashValueBlock = new BytecodeBlock()
                    .append(scope.getVariable("block" + i))
                    .push(0)
                    .push(0)
                    .getVariable(currentBlockLengthVariable)
                    .invokeInterface(io.prestosql.spi.block.Block.class, "hash", int.class, int.class, int.class, int.class)
                    .intToLong()
                    .append(scope.getVariable("range"))
                    .invokeStatic(BigintOperators.class, "modulus", long.class, long.class, long.class)
                    .putVariable(currenHashValueVariable);

            BytecodeBlock updateHashValueBlock = new BytecodeBlock()
                    .getVariable(currenHashValueVariable)
                    .getVariable(hashValueVariable)
                    .invokeStatic(BigintOperators.class, "add", long.class, long.class, long.class)
                    .append(scope.getVariable("range"))
                    .invokeStatic(BigintOperators.class, "modulus", long.class, long.class, long.class)
                    .putVariable(hashValueVariable);

            body.append(currentBlockLength)
                    .append(currentHashValueBlock)
                    .append(updateHashValueBlock);
        }
        body.comment("return hashValue")
                .getVariable(hashValueVariable)
                .append(scope.getVariable("range"))
                .invokeStatic(BigintOperators.class, "add", long.class, long.class, long.class)
                .append(scope.getVariable("range"))
                .invokeStatic(BigintOperators.class, "modulus", long.class, long.class, long.class)
                .ret(long.class);

        return defineClass(definition, Object.class, binder.getBindings(), new DynamicClassLoader(Hash.class.getClassLoader()));
    }
}
