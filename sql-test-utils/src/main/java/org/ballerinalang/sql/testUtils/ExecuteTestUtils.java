package org.ballerinalang.sql.testutils;

import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BObject;

import org.ballerinalang.sql.nativeimpl.ExecuteProcessor;
import org.ballerinalang.sql.parameterprocessor.DefaultStatementParameterProcessor;

public class ExecuteTestUtils {
    private ExecuteTestUtils() {
    }

    public static Object nativeExecute(BObject client, Object paramSQLString) {
        return ExecuteProcessor.nativeExecute(client, paramSQLString, DefaultStatementParameterProcessor.getInstance());
    }

    public static Object nativeBatchExecute(BObject client, BArray paramSQLStrings) {
        return ExecuteProcessor.nativeBatchExecute(client, paramSQLStrings, DefaultStatementParameterProcessor.getInstance());    
    }
}