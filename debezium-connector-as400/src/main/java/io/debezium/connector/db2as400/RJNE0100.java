
package io.debezium.connector.db2as400;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.AS400UnsignedBin4;
import com.ibm.as400.access.FieldDescription;
import com.ibm.as400.access.ProgramParameter;
import com.ibm.as400.access.RecordFormat;

/**
 * This class encapsulates the formatting of RJNE0100 parameters required when using the procedure 
 * QjoRetrieveJournalEntries in service program QSYS/QJOURNAL.  To see the full API document:
 * 
 * http://publib.boulder.ibm.com/infocenter/iseries/v5r4/index.jsp?topic=%2Fapis%2FQJORJRNE.htm
 * 
 * Usage: 
 * 1. Create an instance of this RJNE0100 passing the parameters:
 *   a. Name of the journal.
 *   b. Library where the journal resides.
 *   c. Optional: length of the receiver variable.
 * 2. Invoke addRetrieveCriteria(RetrieveKey eKey, Object value) to add journal entries selection criteria.
 * 3. Invoke getProgramParameters() to return the ProgramParameter[]; in the calling program, pass this array
 *    to ServiceProgramCall.setProgram() method.
 * 
 * After invoke ServiceProgram.run() in the calling program, invoke getReceiver() to return the output data.
 * 
 * @author Stanley Vong
 * @version 1.0 (Nov 23, 2009)
 * Initial creation.
 * 
 * @version 1.1 (Feb 01, 2013)
 * Define RetrieveKey, JournalCode, JournalEntryType as enum. 
 * Create inner class JrneToRtv to handle "Journal Entries to Retrieve" structure.
 *
 */
public class RJNE0100 {
    static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH.mm.ss.SSS");

    public static enum RetrieveKey {
        RCVRNG(1, "Range of journal receivers"),
        FROMENT(2, "Starting sequence number"),
        FROMTIME(3, "Starting time stamp"),
        TOENT(4, "Ending sequence number"),
        TOTIME(5, "Ending time stamp"),
        NBRENT(6, "Number of entries"),
        JRNCDE(7, "Journal codes"),
        ENTTYP(8, "Journal entry types"),
        JOB(9, "Job"),
        PGM(10, "Program"),
        USRPRF(11, "User profile"),
        CMTCYCID(12, "Commit cycle identifier"),
        DEPENT(13, "Dependent entries"),
        INCENT(14, "Include entries"),
        NULLINDLEN(15, "Null value indicators length"),
        FILE(16, "File"),
        OBJ(17, "Object"),
        OBJPATH(18, "Object Path"),
        OBJFID(19, "Object file identifier"),
        SUBTREE(20, "Directory substree"),
        PATTERN(21, "Name pattern"),
        FMTMINDTA(22, "Format Minimized Data");

        private int key;
        private String description;

        private RetrieveKey(int key, String description) {
            this.key = key;
            this.description = description;
        }

        public int getKey() {
            return this.key;
        }

        public String getDescription() {
            return this.description;
        }

        @Override
        public String toString() {
            return String.format("%s, (%d)", getDescription(), getKey());
        }
    };

    public static enum JournalCode {
        A("A", "System accounting entry"),
        B("B", "Integrated file system operation"),
        C("C", "Commitment control operation"),
        D("D", "Database file operation"),
        E("E", "Data area operation"),
        F("F", "Database file member operation"),
        I("I", "Internal operation"),
        J("J", "Journal or journal receiver operation"),
        L("L", "License management"),
        M("M", "Network management data"),
        P("P", "Performance tuning entry"),
        Q("Q", "Data queue operation"),
        R("R", "Record level operation"),
        S("S", "Distributed mail service for SNA distribution services (SNADS), network alerts, or mail server framework"),
        T("T", "Audit trail entry"),
        U("U", "User generated");

        private String key;
        private String description;

        private JournalCode(String key, String description) {
            this.key = key;
            this.description = description;
        }

        public String getKey() {
            return this.key;
        }

        public String getDescription() {
            return this.description;
        }

        @Override
        public String toString() {
            return String.format("%s, (%s)", getDescription(), getKey());
        }
    }

    public static enum JournalEntryType {
        // only Journal Entry Types for Journal Code "R" are included
        BR("BR", "Before-image of record updated for rollback"),
        DL("DL", "Record deleted from physical file member"),
        DR("DR", "Record deleted for rollback"),
        IL("IL", "Increment record limit"),
        PT("PT", "Record added to physical file member"),
        PX("PX", "Record added directly to physical file member"),
        UB("UB", "Before-image of record updated in physical file member"),
        UP("UP", "After-image of record updated in physical file member"),
        UR("UR", "After-image of record updated for rollback");

        private String key;
        private String description;

        private JournalEntryType(String key, String description) {
            this.key = key;
            this.description = description;
        }

        public String getKey() {
            return this.key;
        }

        public String getDescription() {
            return this.description;
        }

        @Override
        public String toString() {
            return String.format("%s, (%s)", getDescription(), getKey());
        }
    }

    public static final int RECEIVER_LEN = 32768;
    public static final String FORMAT_NAME = "RJNE0100";
    public static final int ERROR_CODE = 0;
    private ProgramParameter[] parameterList = null;
    private JrneToRtv criteria = null;
    private Receiver receiver = null;

    /**
     * Constructor providing the name of journal and library where the journal resides.
     * @param journal
     * @param library
     */
    public RJNE0100(String journal, String library) {
        this(journal, library, RECEIVER_LEN);
    }

    /**
     * Constructor providing the name of journal, library where the journal resides and the length of receiver variable.
     * Length of the receiver variable must be 16-byte aligned. 
     * @param journal
     * @param library
     * @param rcvLen
     */
    public RJNE0100(String journal, String library, int rcvLen) {
        // Required Parameter Group:
        // 1 Receiver variable Output Char(*)
        // 2 Length of receiver variable Input Binary(4)
        // 3 Qualified journal name Input Char(20)
        // 4 Format name Input Char(8)
        // Omissible Parameter Group:
        // 5 Journal entries to retrieve Input Char(*)
        // 6 Error code I/O Char(*)

        if (journal == null || journal.trim().length() == 0 || journal.trim().length() > 10) {
            throw new IllegalArgumentException("Journal name must not be null and length must be <= to 10.");
        }
        if (library == null || library.trim().length() == 0 || library.trim().length() > 10) {
            throw new IllegalArgumentException("Library name must not be null and length must be <= to 10.");
        }
        if ((rcvLen % 16) != 0) {
            throw new IllegalArgumentException("Receiver Length not valid; value must be divisable by 16.");
        }

        receiver = new Receiver();
        criteria = new JrneToRtv();
        parameterList = new ProgramParameter[6];

        setReceiver(rcvLen);
        setReceiverLength(rcvLen);
        setJournal(journal, library);
        setFormatName(FORMAT_NAME);
        setJrneToRtv(); // default will retrieve all journal entries
        setErrorCode(ERROR_CODE);
    }

    /**
     * After the program run, invoke getReceiver() to get the instance of the Receiver, which will provide the returning result.
     * @return
     */
    public Receiver getReceiver() {
        return this.receiver;
    }

    private void setReceiver(int rcvLen) {
        parameterList[0] = new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, rcvLen);
    }

    private void setReceiverLength(int rcvLen) {
        parameterList[1] = new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Bin4().toBytes(rcvLen));
    }

    private void setJournal(String journal, String library) {
        String jrnLib = padRight(journal, 10) + padRight(library, 10);
        parameterList[2] = new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Text(20).toBytes(jrnLib));
    }

    private void setFormatName(String format) {
        parameterList[3] = new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Text(8).toBytes(format));
    }

    private void setJrneToRtv() {
        parameterList[4] = new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Structure(criteria.getStructure()).toBytes(criteria.getObject()));
    }

    private void setErrorCode(int error) {
        parameterList[5] = new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Bin4().toBytes(error));
    }

    /**
     * Pad space to the left of the input string s, so that the length of s is n.
     * @param s
     * @param n
     * @return
     */
    public static String padLeft(String s, int n) {
        return String.format("%1$" + n + "s", s);
    }

    /**
     * Pad space to the right of the input string s, so that the length of s is n.
     * @param s
     * @param n
     * @return
     */
    public static String padRight(String s, int n) {
        return String.format("%1$-" + n + "s", s);
    }

    /**
     * Return the 6-elements array of ProgramParameter to pass to the ProgramCall/ServiceProgramCall. 
     * @return
     */
    public ProgramParameter[] getProgramParameters() {
        return parameterList;
    }

    /**
     * Add journal retrieve criteria.
     * @param eKey
     * @param value
     */
    public void addRetrieveCriteria(RetrieveKey eKey, Object value) {
        if (eKey == null) {
            throw new IllegalArgumentException("RetrieveKey cannot be null.");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null.");
        }
        switch (eKey) {
            case RCVRNG:
                criteria.addRcvRng(value);
                break;
            case FROMENT:
                criteria.addFromEnt(value);
                break;
            case NBRENT:
                criteria.addNbrEnt(value);
                break;
            case JRNCDE:
                criteria.addJrnCde(value);
                break;
            case ENTTYP:
                criteria.addEntTyp(value);
                break;
            default:
                throw new IllegalArgumentException(String.format("Method not yet implemented for RetreieveKey '%s'", eKey.toString()));
        }
        ;
        parameterList[4] = new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Structure(criteria.getStructure()).toBytes(criteria.getObject()));
    }

    /**
     * Inner class representing the receiver variable.  Mainly used to decode the following:
     * a. 1-time journal header
     * b. Repetitive occurrence of the entry sections
     *    b1. the entry header section
     *    b2. the entry null section
     *    b3. the entry detail section
     * 
     * @author Stanley
     *
     */
    public class Receiver {
        private Logger log = LoggerFactory.getLogger(Receiver.class);

        private AS400Structure headerStructure = null;
        private AS400Structure entryHeaderStructure = null;
        private AS400Structure entryDetailStructure = null;
        private int entryHeaderStartPos = -1;
        private int entryRRN = 0;

        private AS400Structure getHeaderStructure() {
            if (this.headerStructure == null) {
                AS400DataType[] structure = {
                        new AS400Bin4(),
                        new AS400Bin4(),
                        new AS400Bin4(),
                        new AS400Text(1)
                };
                this.headerStructure = new AS400Structure(structure);
            }
            return this.headerStructure;
        }

        private AS400Structure getEntryHeaderStructure() {
            if (this.entryHeaderStructure == null) {
                AS400DataType[] structure = {
                        new AS400Bin4(),
                        new AS400Bin4(),
                        new AS400Bin4(),
                        new AS400UnsignedBin4(),
                        new AS400Text(20),
                        new AS400Text(1),
                        new AS400Text(2),
                        new AS400Text(26),
                        new AS400Text(10),
                        new AS400Text(10),
                        new AS400Text(6),
                        new AS400Text(10),
                        new AS400Text(30),
                        new AS400Text(10),
                        new AS400Text(1),
                        new AS400Text(20),
                        new AS400Text(10),
                        new AS400Text(8),
                        new AS400Text(10), // id
                        new AS400Text(1),
                        new AS400Text(1),
                        new AS400Text(1),
                        new AS400Text(1),
                        new AS400Text(1),
                        new AS400Text(1)
                };
                this.entryHeaderStructure = new AS400Structure(structure);
            }
            return this.entryHeaderStructure;
        }

        private AS400Structure getEntrySpecificDataStructure() {
            // if (this.entryDetailStructure == null){
            AS400DataType[] structure = { new AS400Text(5), new AS400Text(11) };
            this.entryDetailStructure = new AS400Structure(structure);
            // }
            return this.entryDetailStructure;
        }

        private AS400Structure getEntrySpecificDataStructure(RecordFormat recFormat) {
            // if (this.entryDetailStructure == null){
            ArrayList<AS400DataType> structure = new ArrayList<AS400DataType>();
            structure.add(new AS400Text(5));
            structure.add(new AS400Text(11));
            FieldDescription[] fds = recFormat.getFieldDescriptions();
            for (int i = 0; i < fds.length; i++) {
                structure.add(fds[i].getDataType());
            }
            this.entryDetailStructure = new AS400Structure(structure.toArray(new AS400DataType[0]));
            // }
            return this.entryDetailStructure;
        }

        /**
         * Point to the next journal entry (section b) in the receiver variable.
         * @return
         */
        public boolean nextEntry() {
            if (entryRRN < getNbrOfEntriesRetrieved()) {
                entryRRN++;
                if (entryRRN == 1) {
                    entryHeaderStartPos = getOffsetToFirstJrneHeader();
                }
                else {
                    entryHeaderStartPos += getDspToNxtJrnEntHdr();
                }
                return true;
            }
            else {
                return false;
            }
        }

        /**
         * Get number of bytes returned in the journal header (section a).
         * @return 
         */
        public int getBytesReturned() {
            Object[] result = (Object[]) receiver.getHeaderStructure().toObject(parameterList[0].getOutputData(), 0);
            return (Integer) result[0];
        }

        /**
         * Get offset to the first journal entry header in the journal header (section a).
         * @return
         */
        public int getOffsetToFirstJrneHeader() {
            Object[] result = (Object[]) receiver.getHeaderStructure().toObject(parameterList[0].getOutputData(), 0);
            return (Integer) result[1];
        }

        /**
         * Get number of entries retrieved in the journal header (section a).
         * @return
         */
        public int getNbrOfEntriesRetrieved() {
            Object[] result = (Object[]) receiver.getHeaderStructure().toObject(parameterList[0].getOutputData(), 0);
            return (Integer) result[2];
        }

        /**
         * Get continuation handle in the journal header (section a).
         * @return
         */
        public String getContinuationHandle() {
            Object[] result = (Object[]) receiver.getHeaderStructure().toObject(parameterList[0].getOutputData(), 0);
            return (String) result[3];
        }

        /**
         * Get displacement to next journal entry's header in the journal entry header section (section b1)
         * @return
         */
        public int getDspToNxtJrnEntHdr() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (Integer) result[0];
        }

        /**
         * Get displacement to this journal entry's null value indicator in the journal entry header section (section b1)
         * @return
         */
        public int getDspToThsJrnEntNullValInd() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (Integer) result[1];
        }

        /**
         * Get displacement to this journal entry's specific data in the journal entry header section (section b1)
         * @return
         */
        public int getDspToThsJrnEntData() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (Integer) result[2];
        }

        /**
         * Get pointer handle in section b1
         * @return
         */
        public int getPointerHandle() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (Integer) result[3];
        }

        /**
         * Get sequence number in section b1
         * @return
         */
        public String getSequenceNumber() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[4];
        }

        /**
         * Get journal code in section b1
         * @return
         */
        public String getJournalCode() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[5];
        }

        /**
         * Get entry type in section b1
         * @return
         */
        public String getEntryType() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[6];
        }

        /**
         * Get timestamp in section b1
         * @return 2020-04-29-16.53.41.815360
        * @throws ParseException 
         */
        public Date getEntryDateOrNow() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            String time = (String) result[7];
            try {
                return RJNE0100.DATE_FORMAT.parse(time);
            }
            catch (ParseException e) {
                log.warn("problem parsing date: {}", time, e);
                return new Date();
            }
        }

        public String getEntryDateString() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[7];
        }

        /**
         * Get job name in section b1
         * @return
         */
        public String getJobName() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[8];
        }

        /**
         * Get user name in section b1
         * @return
         */
        public String getUserName() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[9];
        }

        /**
         * Get job number in section b1
         * @return
         */
        public String getJobNumber() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[10];
        }

        /**
         * Get program name in section b1
         * @return
         */
        public String getProgramName() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[11];
        }

        /**
         * Get object in section b1
         * @return
         */
        public String getObject() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[12];
        }

        /**
         * Get count/relative record number in section b1
         * @return
         */
        public String getCountRRN() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[13];
        }

        /**
         * Get indicator flag in section b1
         * @return
         */
        public String getIndicatorFlag() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[14];
        }

        /**
         * Get commit cycle id in section b1
         * @return
         */
        public String getCommitCycleId() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[15];
        }

        /**
         * Get user profile in section b1
         * @return
         */
        public String getUserProfile() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[16];
        }

        /**
         * Get system name in section b1
         * @return
         */
        public String getSystemName() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[17];
        }

        /**
         * Get journal identifier in section b1
         * @return
         */
        public String getJournalId() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[18];
        }

        // public String getJournalId2(){
        // Object[] result = (Object[])receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
        // return Long.valueOf(result[18]).toString();
        // }

        /**
         * Get referential constraint in section b1
         * @return
         */
        public String getReferentialConstraint() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[19];
        }

        /**
         * Get trigger in section b1
         * @return
         */
        public String getTrigger() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[20];
        }

        /**
         * Get incomplete data in section b1
         * @return
         */
        public String getIncomepleteData() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[21];
        }

        /**
         * Get object name indicator in section b1
         * @return
         */
        public String getObjectNameIndicator() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[22];
        }

        /**
         * Get ignore during APYJRNCHG or RMVJRNCHG in section b1
         * @return
         */
        public String getIgnoreApyRmvJrnChg() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[23];
        }

        /**
         * Get minimized entry specific data in section b1
         * @return
         */
        public String getMinEntrySpecificData() {
            Object[] result = (Object[]) receiver.getEntryHeaderStructure().toObject(parameterList[0].getOutputData(), entryHeaderStartPos);
            return (String) result[24];
        }

        /**
         * Get length of entry specific data length in section b3
         * @return
         */
        public String getEntrySpecificDataLength() {
            Object[] result = (Object[]) receiver.getEntrySpecificDataStructure().toObject(parameterList[0].getOutputData(),
                    entryHeaderStartPos + getDspToThsJrnEntData());
            return (String) result[0];
        }

        /**
         * Get length of entry specific data length in section b3
         * @return
         */
        public String getEntrySpecificDataLength(DynamicRecordFormat recFormat) {
            Object[] result = (Object[]) receiver.getEntrySpecificDataStructure(recFormat).toObject(parameterList[0].getOutputData(),
                    entryHeaderStartPos + getDspToThsJrnEntData());
            return (String) result[0];
        }

        public Object[] getEntrySpecificData(RecordFormat recFormat) {
            Object[] result = (Object[]) receiver.getEntrySpecificDataStructure(recFormat).toObject(parameterList[0].getOutputData(),
                    entryHeaderStartPos + getDspToThsJrnEntData());
            Object[] result2 = null;
            if (result != null && result.length > 2) {
                ArrayList<Object> list = new ArrayList<Object>();
                for (int i = 2; i < result.length; i++) {
                    list.add(result[i]);
                }
                result2 = list.toArray();
            }
            else {
                result2 = new Object[0];
            }
            return result2;
        }
    }

    /**
     * Inner class representing Journal Entry to Retrieve section.  Mainly used to encode the following:
     * a.  Binary(4) - Number of Variable Length Records
     * b1. Binary(4) - Length of Variable Length Record - length(b1+b2+b3+b4)
     * b2. Binary(4) - Key
     * b3. Binary(4) - Length of Data - length(b4)
     * b4. Char(*)   - Data
     * 
     * To see an example using AS400Structure for a composite type of data types:
     * http://publib.boulder.ibm.com/html/as400/java/rzahh115.htm#HDRRZAHH-COMEX
     * 
     * @author Stanley
     * 
     */
    private class JrneToRtv {
        private ArrayList<AS400DataType> structure = null;
        private ArrayList<Object> data = null;

        private JrneToRtv() {
            // first element is the number of variable length records
            structure = new ArrayList<AS400DataType>();
            structure.add(new AS400Bin4());
            data = new ArrayList<Object>();
            data.add(new Integer(0));
        }

        private AS400DataType[] getStructure() {
            return structure.toArray(new AS400DataType[0]);
        }

        private Object[] getObject() {
            return data.toArray(new Object[0]);
        }

        /**
         * Add retrieval criteria 01: range of journal receivers.  This can be used to indicate where to start when previous
         * returned continuation handle='1'.
         * @param value
         */
        private void addRcvRng(Object value) {
            RetrieveKey curKey = RetrieveKey.RCVRNG;
            String temp = null;
            if (value instanceof String) {
                temp = ((String) value).trim();
                if (temp.equals("*CURCHAIN") || temp.equals("*CURRENT")) {
                    temp = padRight(temp, 40);
                }
                else {
                    throw new IllegalArgumentException(String.format("Value for '%s' must be either '*CURCHAIN' or '*CURRENT' if String; " +
                            "or an instance of String[] with four elements (in the order of: starting receiver, staring library, " +
                            "ending receiver, ending library).", curKey.getDescription()));
                }
            }
            else if (value instanceof String[]) {
                String[] range = (String[]) value;
                if (range.length == 4) {
                    String strRcv = range[0];
                    String strLib = range[1];
                    String endRcv = range[2];
                    String endLib = range[3];
                    temp = padRight(strRcv, 10) + padRight(strLib, 10) + padRight(endRcv, 10) + padRight(endLib, 10);
                }
                else {
                    throw new IllegalArgumentException(String.format("Value for '%s' must be either '*CURCHAIN' or '*CURRENT' if String; " +
                            "or an instance of String[] with four elements (in the order of: starting receiver, staring library, " +
                            "ending receiver, ending library).", curKey.getDescription()));
                }
            }
            else {
                throw new IllegalArgumentException(String.format("Value for '%s' must be either '*CURCHAIN' or '*CURRENT' if String; " +
                        "or an instance of String[] with four elements (in the order of: starting receiver, staring library, " +
                        "ending receiver, ending library).", curKey.getDescription()));
            }

            addStructureData(curKey, new AS400Text(40), temp);
        }

        /**
         * Add retrieval criteria 02: starting sequence number.  This can be used to indicate where to start when previous 
         * returned continuation handle='1'.
         * @param value
         */
        private void addFromEnt(Object value) {
            RetrieveKey curKey = RetrieveKey.FROMENT;
            String temp = null;
            if (value instanceof String) {
                temp = ((String) value).trim();
                if (temp.equals("*FIRST")) {
                    temp = padRight(temp, 20);
                }
                else {
                    throw new IllegalArgumentException(String.format("Value for '%s' must be either '*FIRST' or an instance of Integer.", curKey.getDescription()));
                }
            }
            else if (value instanceof Integer || value instanceof Long) {
                temp = value.toString();
                // integer will be passed as String, need to padLeft()
                temp = padLeft(temp, 20);
            }
            else {
                throw new IllegalArgumentException(String.format("Value for '%s' must be either '*FIRST' or an instance of Integer.", curKey.getDescription()));
            }

            addStructureData(curKey, new AS400Text(20), temp);
        }

        /**
         * Add retrieval criteria 06: max number of entries to retrieve.  This indicates the 'max' number of entries to retrieve, not 
         * number of entries retrieved in this call. 
         * @param eKey
         * @param value
         */
        private void addNbrEnt(Object value) {
            RetrieveKey curKey = RetrieveKey.NBRENT;
            if (!(value instanceof Integer)) {
                throw new IllegalArgumentException(String.format("Value for '%s' must be an instance of Integer.", curKey.getDescription()));
            }

            addStructureData(curKey, new AS400Bin4(), value);
        }

        /**
         * Add retrieval criteria 07: journal codes.  Input parameter must be one of the below:
         * - *ALL (String).
         * - *CTL (String).
         * - JournalCode[] to hold all the desired journal codes to retrieve.  Currently only '*ALLSLT' is implemented if JournalCode[] is passed in. 
         * @param value
         */
        private void addJrnCde(Object value) {
            RetrieveKey curKey = RetrieveKey.JRNCDE;
            String temp = null;
            int count = 0;
            if (value instanceof String) {
                temp = ((String) value).trim();
                if (temp.equals("*ALL") || temp.equals("*CTL")) {
                    temp = padRight(temp, 20);
                    count = 1;
                }
                else {
                    throw new IllegalArgumentException(String.format("Value for '%s' must be either '*ALL' or '*CTL' if String; " +
                            "or an instance of JournalCode[] containing the desired journal codes to retrieve.", curKey.getDescription()));
                }
            }
            else if (value instanceof JournalCode[]) {
                JournalCode[] range = (JournalCode[]) value;
                StringBuilder code = new StringBuilder();
                for (int i = 0; i < range.length; i++) {
                    code.append(padRight(range[i].getKey(), 10));
                    code.append(padRight("*ALLSLT", 10));
                }
                temp = code.toString();
                count = range.length;
            }
            else {
                throw new IllegalArgumentException(String.format("Value for '%s' must be either '*ALL' or '*CTL' if String; " +
                        "or an instance of JournalCode[] containing the desired journal codes to retrieve.", curKey.getDescription()));
            }

            Object[] temp2 = new Object[2];
            temp2[0] = new Integer(count);
            temp2[1] = temp;

            AS400DataType type[] = new AS400DataType[2];
            type[0] = new AS400Bin4();
            type[1] = new AS400Text(temp.length());
            AS400Structure temp2Structure = new AS400Structure(type);

            addStructureData(curKey, temp2Structure, temp2);
        }

        /**
         * Add retrieval criteria 08: journal entry types.
         * @param value
         */
        private void addEntTyp(Object value) {
            RetrieveKey curKey = RetrieveKey.ENTTYP;
            String temp = null;
            int count = 0;
            if (value instanceof String) {
                temp = ((String) value).trim();
                if (temp.equals("*ALL") || temp.equals("*RCD")) {
                    temp = padRight(temp, 10);
                    count = 1;
                }
                else {
                    throw new IllegalArgumentException(String.format("Value for '%s' must be either '*ALL' or '*RCD' if String; " +
                            "or an instance of JournalEntryType[] containing the desired entry types to retrieve.", curKey.getDescription()));
                }
            }
            else if (value instanceof JournalEntryType[]) {
                JournalEntryType[] range = (JournalEntryType[]) value;
                StringBuilder entry = new StringBuilder();
                for (int i = 0; i < range.length; i++) {
                    entry.append(padRight(range[i].getKey(), 10));
                }
                temp = entry.toString();
                count = range.length;

            }
            else {
                throw new IllegalArgumentException(String.format("Value for '%s' must be either '*ALL' or '*RCD' if String; " +
                        "or an instance of JournalEntryType[] containing the desired entry types to retrieve.", curKey.getDescription()));
            }

            Object[] temp2 = new Object[2];
            temp2[0] = new Integer(count);
            temp2[1] = temp;

            AS400DataType type[] = new AS400DataType[2];
            type[0] = new AS400Bin4();
            type[1] = new AS400Text(temp.length());
            AS400Structure temp2Structure = new AS400Structure(type);

            addStructureData(curKey, temp2Structure, temp2);
        }

        /**
         * Add additional selection entry to two ArrayList: structure and data.
         * @param rKey
         * @param dataType
         * @param value
         */
        private void addStructureData(RetrieveKey rKey, AS400DataType dataType, Object value) {
            AS400Bin4 parm1Type = new AS400Bin4();
            AS400Bin4 parm2Type = new AS400Bin4();
            AS400Bin4 parm3Type = new AS400Bin4();
            AS400DataType parm4Type = dataType;

            Integer parm1Value = new Integer(parm1Type.getByteLength() + parm2Type.getByteLength() + parm3Type.getByteLength() + parm4Type.getByteLength());
            Integer parm2Value = new Integer(rKey.getKey());
            Integer parm3Value = new Integer(parm4Type.getByteLength());
            Object parm4Value = value;

            structure.add(parm1Type);
            structure.add(parm2Type);
            structure.add(parm3Type);
            structure.add(parm4Type);

            data.add(parm1Value);
            data.add(parm2Value);
            data.add(parm3Value);
            data.add(parm4Value);

            // pump up "Number of Variable Length Records" by 1
            data.set(0, (Integer) data.get(0) + 1);
        }
    }

}
