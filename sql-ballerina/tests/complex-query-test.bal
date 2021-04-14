// Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/lang.'string as strings;
import ballerina/test;
import ballerina/time;

string complexQueryDb = urlPrefix + "9008/querycomplexparams";

@test:BeforeGroups {
 	value: ["query-complex-params"]
} 
function initQueryComplexContainer() returns error? {
 	check initializeDockerContainer("sql-query-complex", "querycomplexparams", "9008", "query", "complex-test-data.sql");
}

@test:AfterGroups {
 	value: ["query-complex-params"]
} 
function cleanQueryComplexContainer() returns error? {
	check cleanDockerContainer("sql-query-complex");
}

type SelectTestAlias record {
    int int_type;
    int long_type;
    float double_type;
    boolean boolean_type;
    string string_type;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}

function testGetPrimitiveTypes() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query(
	"SELECT int_type, long_type, double_type,"
        + "boolean_type, string_type from DataTable WHERE row_id = 1");
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
    check dbClient.close();

    SelectTestAlias expectedData = {
        int_type: 1,
        long_type: 9223372036854774807,
        double_type: 2139095039,
        boolean_type: true,
        string_type: "Hello"
    };
    test:assertEquals(value, expectedData, "Expected data did not match.");
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testToJson() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query(
	"SELECT int_type, long_type, double_type, boolean_type, string_type from DataTable WHERE row_id = 1");
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
    json retVal = check value.cloneWithType(json);
    SelectTestAlias expectedData = {
        int_type: 1,
        long_type: 9223372036854774807,
        double_type: 2139095039,
        boolean_type: true,
        string_type: "Hello"
    };
    json|error expectedDataJson = expectedData.cloneWithType(json);
    if (expectedDataJson is json) {
        test:assertEquals(retVal, expectedDataJson, "Expected JSON did not match.");
    } else {
        test:assertFail("Error in cloning record to JSON" + expectedDataJson.message());
    }

    check dbClient.close();
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testToJsonComplexTypes() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query("SELECT blob_type,clob_type,binary_type from" +
        " ComplexTypes where row_id = 1");
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
    check dbClient.close();

    var complexStringType = {
        blob_type: "wso2 ballerina blob test.".toBytes(),
        clob_type: "very long text",
        binary_type: "wso2 ballerina binary test.".toBytes()
    };
    test:assertEquals(value, complexStringType, "Expected record did not match.");
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testComplexTypesNil() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query("SELECT blob_type,clob_type,binary_type from " +
        " ComplexTypes where row_id = 2");
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
    check dbClient.close();
    var complexStringType = {
        blob_type: (),
        clob_type: (),
        binary_type: ()
    };
    test:assertEquals(value, complexStringType, "Expected record did not match.");
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testArrayRetrieval() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query("SELECT int_type, int_array, long_type, long_array, " +
        "boolean_type, string_type, string_array, boolean_array " +
        "from MixTypes where row_id =1");
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
    check dbClient.close();

    float[] doubleTypeArray = [245.23, 5559.49, 8796.123];
    var mixTypesExpected = {
        int_type: 1,
        int_array: [1, 2, 3],
        long_type:9223372036854774807,
        long_array: [100000000, 200000000, 300000000],
        boolean_type: true,
        string_type: "Hello",
        string_array: ["Hello", "Ballerina"],
        boolean_array: [true, false, true]
    };
    test:assertEquals(value, mixTypesExpected, "Expected record did not match.");
}

type TestTypeData record {
    int int_type;
    int[] int_array;
    int long_type;
    int[] long_array;
    boolean boolean_type;
    string string_type;
    string[] string_array;
    boolean[] boolean_array;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testComplexWithStructDef() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query("SELECT int_type, int_array, long_type, long_array, "
        + "boolean_type, string_type, boolean_array, string_array "
        + "from MixTypes where row_id =1", TestTypeData);
    record {|record {} value;|}? data = check streamData.next();
    check streamData.close();
    record {}? value = data?.value;
    check dbClient.close();
    TestTypeData mixTypesExpected = {
        int_type: 1,
        int_array: [1, 2, 3],
        long_type: 9223372036854774807,
        long_array:[100000000, 200000000, 300000000],
        boolean_type: true,
        string_type: "Hello",
        boolean_array: [true, false, true],
        string_array: ["Hello", "Ballerina"]
    };
    test:assertEquals(value, mixTypesExpected, "Expected record did not match.");
}

type ResultMap record {
    int[] int_array;
    int[] long_array;
    boolean[] boolean_array;
    string[] string_array;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testMultipleRecoredRetrieval() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> streamData = dbClient->query("SELECT int_array, long_array, boolean_array," +
        "string_array from ArrayTypes", ResultMap);

    ResultMap mixTypesExpected = {
        int_array: [1, 2, 3],
        long_array: [100000000, 200000000, 300000000],
        string_array: ["Hello", "Ballerina"],
        boolean_array: [true, false, true]
    };

    ResultMap? mixTypesActual = ();
    int counter = 0;
    error? e = streamData.forEach(function (record {} value) {
        if (value is ResultMap && counter == 0) {
            mixTypesActual = value;
        }
        counter = counter + 1;
    });
    if (e is error) {
        test:assertFail("Error when iterating through records " + e.message());
    }
    test:assertEquals(mixTypesActual, mixTypesExpected, "Expected record did not match.");
    test:assertEquals(counter, 4);
    check dbClient.close();

}

type ResultDates record {
    string date_type;
    string time_type;
    string timestamp_type;
    string datetime_type;
    string time_tz_type;
    string timestamp_tz_type;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testDateTime() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> queryResult = dbClient->query("SELECT date_type, time_type, timestamp_type, datetime_type"
       + ", time_tz_type, timestamp_tz_type from DateTimeTypes where row_id = 1", ResultDates);
    record{| record{} value; |}? data =  check queryResult.next();
    record{}? value = data?.value;
    check dbClient.close();

    string dateTypeString = "2017-05-23";
    string timeTypeString = "14:15:23";
    string timestampTypeString = "2017-01-25 16:33:55.0";
    string timeWithTimezone = "16:33:55+06:30";
    string timestampWithTimezone = "2017-01-25T16:33:55-08:00";

    ResultDates expected = {
        date_type: dateTypeString,
        time_type: timeTypeString,
        timestamp_type: timestampTypeString,
        datetime_type: timestampTypeString,
        time_tz_type: timeWithTimezone,
        timestamp_tz_type: timestampWithTimezone
    };
    test:assertEquals(value, expected, "Expected record did not match.");
}

type ResultDates2 record {
    time:Date date_type;
    time:TimeOfDay time_type;
    time:Civil timestamp_type;
    time:Civil datetime_type;
    time:TimeOfDay time_tz_type;
    time:Civil timestamp_tz_type;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testDateTime2() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> queryResult = dbClient->query("SELECT date_type, time_type, timestamp_type, datetime_type, "
            + "time_tz_type, timestamp_tz_type from DateTimeTypes where row_id = 1", ResultDates2);
    record{| record{} value; |}? data =  check queryResult.next();
    record{}? value = data?.value;
    check dbClient.close();

    time:Date dateTypeRecord = {year: 2017, month: 5, day: 23};
    time:TimeOfDay timeTypeRecord = {hour: 14, minute: 15, second:23};
    time:Civil timestampTypeRecord = {year: 2017, month: 1, day: 25, hour: 16, minute: 33, second: 55};
    time:TimeOfDay timeWithTimezone = {utcOffset: {hours: 6, minutes: 30}, hour: 16, minute: 33, second: 55, "timeAbbrev": "+06:30"};
    time:Civil timestampWithTimezone = {utcOffset: {hours: -8, minutes: 0}, timeAbbrev: "-08:00", year:2017,
                                        month:1, day:25, hour: 16, minute: 33, second:55};

    ResultDates2 expected = {
        date_type: dateTypeRecord,
        time_type: timeTypeRecord,
        timestamp_type: timestampTypeRecord,
        datetime_type: timestampTypeRecord,
        time_tz_type: timeWithTimezone,
        timestamp_tz_type: timestampWithTimezone
    };
    test:assertEquals(value, expected, "Expected record did not match.");
}

type RandomType record {|
    int x;
|};

type ResultDates3 record {
    RandomType date_type;
    RandomType time_type;
    RandomType timestamp_type;
    RandomType datetime_type;
    RandomType time_tz_type;
    RandomType timestamp_tz_type;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testDateTime3() returns error? {
    stream<record{}, error?> queryResult;
    record {|record {} value;|} | error? result;
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);

    queryResult = dbClient->query("SELECT date_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Exected for Date type.");
    test:assertTrue(strings:includes((<error>result).message(), "Unsupported Ballerina type"), "Wrong Error Message for Date type.");

    queryResult = dbClient->query("SELECT time_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Exected for Time type.");
    test:assertTrue(strings:includes((<error>result).message(), "Unsupported Ballerina type"), "Wrong Error Message for Time type.");

    queryResult = dbClient->query("SELECT timestamp_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Exected for Timestamp type.");
    test:assertTrue(strings:includes((<error>result).message(), "Unsupported Ballerina type"), "Wrong Error Message for Timestamp type.");

    queryResult = dbClient->query("SELECT datetime_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Exected for Datetime type.");
    test:assertTrue(strings:includes((<error>result).message(), "Unsupported Ballerina type"), "Wrong Error Message for Datetime type.");

    queryResult = dbClient->query("SELECT time_tz_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Exected for Time with Timezone type.");
    test:assertTrue(strings:includes((<error>result).message(), "Unsupported Ballerina type"), "Wrong Error Message for Time with Timezone type.");
    
    queryResult = dbClient->query("SELECT timestamp_tz_type from DateTimeTypes where row_id = 1", ResultDates3);
    result = queryResult.next();
    test:assertTrue(result is error, "Error Exected for Timestamp with Timezone type.");
    test:assertTrue(strings:includes((<error>result).message(), "Unsupported Ballerina type"), "Wrong Error Message for Timestamp with Timezone type.");
    check dbClient.close();
}

type ResultSetTestAlias record {
    int int_type;
    int long_type;
    string float_type;
    float double_type;
    boolean boolean_type;
    string string_type;
    int dt2int_type;
};

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testColumnAlias() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    stream<record{}, error?> queryResult = dbClient->query("SELECT dt1.int_type, dt1.long_type, dt1.float_type," +
           "dt1.double_type,dt1.boolean_type, dt1.string_type,dt2.int_type as dt2int_type from DataTable dt1 " +
           "left join DataTableRep dt2 on dt1.row_id = dt2.row_id WHERE dt1.row_id = 1;", ResultSetTestAlias);
    ResultSetTestAlias expectedData = {
        int_type: 1,
        long_type: 9223372036854774807,
        float_type: "123.34",
        double_type: 2139095039,
        boolean_type: true,
        string_type: "Hello",
        dt2int_type: 100
    };
    int counter = 0;
    error? e = queryResult.forEach(function (record{} value) {
        if (value is ResultSetTestAlias) {
            test:assertEquals(value, expectedData, "Expected record did not match.");
            counter = counter + 1;
        } else{
            test:assertFail("Expected data type is ResultSetTestAlias");
        }
    });
    if(e is error) {
        test:assertFail("Query failed");
    }
    test:assertEquals(counter, 1, "Expected only one data row.");
    check dbClient.close();
}

@test:Config {
    groups: ["query", "query-complex-params"]
}
function testQueryRowId() returns error? {
    MockClient dbClient = check new (url = complexQueryDb, user = user, password = password);
    ExecutionResult result = check dbClient->execute("SET DATABASE SQL SYNTAX ORA TRUE");
    stream<record{}, error?> streamData = dbClient->query("SELECT rownum, int_array, long_array, boolean_array," +
         "string_array from ArrayTypes");

    record{} mixTypesExpected = {
        "rownum": 1,
        "int_array": [1, 2, 3],
        "long_array": [100000000, 200000000, 300000000],
        "boolean_array": [true, false, true],
        "string_array": ["Hello", "Ballerina"]
    };

    record{}? mixTypesActual = ();
    int counter = 0;
    error? e = streamData.forEach(function (record {} value) {
        if (counter == 0) {
            mixTypesActual = value;
        }
        counter = counter + 1;
    });
    if (e is error) {
        test:assertFail("Query failed");
    }
    test:assertEquals(mixTypesActual, mixTypesExpected, "Expected record did not match.");
    test:assertEquals(counter, 4);
    check dbClient.close();
}
