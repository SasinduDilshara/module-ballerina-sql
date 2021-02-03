package org.ballerinalang.sql.utils.connector;

import io.ballerina.runtime.api.values.BObject;
import org.ballerinalang.sql.utils.ProcedureCallResultUtils;

/**
 * This class holds the utility methods involved with executing the call statements.
 */
public class CustomProcedureResultUtils extends ProcedureCallResultUtils {

    public static Object getNextQueryResult(BObject procedureCallResultset, BObject procedureCallResult) {
        return ProcedureCallResultUtils.getNextQueryResult(procedureCallResult);
    }
}
