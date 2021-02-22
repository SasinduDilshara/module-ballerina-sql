package org.ballerinalang.sql.testutils;

import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import org.ballerinalang.sql.datasource.SQLDatasource;

import org.ballerinalang.sql.nativeimpl.ClientProcessor;

public class ClientTestUtils {
    
    private ClientTestUtils(){ 
    }

    public static Object createSqlClient(BObject client, BMap<BString, Object> sqlDatasourceParams,
                                         BMap<BString, Object> globalConnectionPool) {
        return ClientProcessor.createClient(client, SQLDatasource.createSQLDatasourceParams(sqlDatasourceParams, globalConnectionPool));
    }

    public static Object close(BObject client) {
        return ClientProcessor.close(client);
    }
}