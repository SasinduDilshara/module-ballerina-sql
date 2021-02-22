package org.ballerinalang.sql.testutils;

import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BObject;

import org.ballerinalang.sql.nativeimpl.CallProcessor;
import org.ballerinalang.sql.parameterprocessor.DefaultResultParameterProcessor;
import org.ballerinalang.sql.parameterprocessor.DefaultStatementParameterProcessor;

public class CallTestUtils {
    private CallTestUtils(){ 
    }
    
    public static Object nativeCall(BObject client, Object paramSQLString, BArray recordTypes) {
        return CallProcessor.nativeCall(client, paramSQLString, recordTypes, DefaultStatementParameterProcessor.getInstance(),
            DefaultResultParameterProcessor.getInstance());
    }
}