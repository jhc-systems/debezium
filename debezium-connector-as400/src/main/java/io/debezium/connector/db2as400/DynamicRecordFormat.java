package io.debezium.connector.db2as400;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.ibm.as400.access.*;

/**
 * 
 * @author Stanley Vong
 * 
 * 
 * @version 1.0 (March 28, 2005)
 * Initial Creation.
 * 
 */

public class DynamicRecordFormat extends RecordFormat {

    // cache the table formats
    static Map<String, DynamicRecordFormat> recordFormatMap = new HashMap<>();

    static boolean hasRecordFormat(String name) {
        return recordFormatMap.containsKey(name);
    }

    // TODO need to update the record format when table changes
    static DynamicRecordFormat getRecordFormat(String name, AS400 as400)
            throws AS400Exception, AS400SecurityException, InterruptedException, IOException {
        if (recordFormatMap.containsKey(name)) {
            return recordFormatMap.get(name);
        }
        DynamicRecordFormat objPath = new DynamicRecordFormat(as400, new QSYSObjectPathName(name));
        recordFormatMap.put(name, objPath);
        return objPath;
    }

    /**
     * Create an instance of RecordFormat using the AS400 and QSYSObjectPathName as reference.
     * @param as400
     * @param filePathName
     * @throws AS400Exception
     * @throws AS400SecurityException
     * @throws InterruptedException
     * @throws IOException
     */
    public DynamicRecordFormat(AS400 as400, QSYSObjectPathName filePathName)
            throws AS400Exception, AS400SecurityException, InterruptedException, IOException {

        super(filePathName.getObjectName());
        AS400FileRecordDescription recordDescription = new AS400FileRecordDescription(as400, filePathName.getPath());
        RecordFormat[] fileFormat = recordDescription.retrieveRecordFormat();

        if (fileFormat.length > 1) {
            throw new IllegalArgumentException(filePathName.getPath() + " contains more than one record format.");
        }
        // always default to fileFormat[0] cause only physical files are assumed here.
        FieldDescription fd = null;
        for (int i = 0; i < fileFormat[0].getNumberOfFields(); i++) {
            fd = fileFormat[0].getFieldDescription(i);
            addFieldDescription(fd);
        }
        String[] keys = fileFormat[0].getKeyFieldNames();
        for (String key : keys) {
            addKeyFieldDescription(key);
        }
    }
}