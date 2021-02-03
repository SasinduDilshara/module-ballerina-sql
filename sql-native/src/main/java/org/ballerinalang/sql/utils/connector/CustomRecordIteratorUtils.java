package org.ballerinalang.sql.utils.connector;

import io.ballerina.runtime.api.values.BObject;
import org.ballerinalang.sql.utils.RecordIteratorUtils;

/**
 * This class holds the utility methods involved with executing the call statements.
 */
public class CustomRecordIteratorUtils extends RecordIteratorUtils {

    public static Object nextResult(BObject connectorRecordIterator, BObject recordIterator) {
        return RecordIteratorUtils.nextResult(recordIterator);
    }
    
}
