package org.ballerinalang.sql.testutils;

import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;

import org.ballerinalang.sql.nativeimpl.QueryProcessor;
import org.ballerinalang.sql.parameterprocessor.DefaultResultParameterProcessor;
import org.ballerinalang.sql.parameterprocessor.DefaultStatementParameterProcessor;

public class QueryTestUtils {

    private QueryTestUtils() {
    }

    public static BStream nativeQuery(BObject client, Object paramSQLString,
                                      Object recordType) {
        DefaultStatementParameterProcessor statementParametersProcessor = DefaultStatementParameterProcessor
                .getInstance();
        DefaultResultParameterProcessor resultParametersProcessor = DefaultResultParameterProcessor
                .getInstance();
        return QueryProcessor.nativeQuery(client, paramSQLString, recordType, statementParametersProcessor, resultParametersProcessor);
    }
}
