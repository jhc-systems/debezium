package io.debezium.connector.db2as400;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.ServiceProgramCall;

import io.debezium.connector.db2as400.RJNE0100.Receiver;
import io.debezium.connector.db2as400.RJNE0100.RetrieveKey;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;

/**
 * {@link JdbcConnection} extension to be used with IBM Db2
 *
 * @author Horia Chiorean (hchiorea@redhat.com), Jiri Pechanec, Peter Urbanetz
 *
 */
public class As400RpcConnection implements AutoCloseable {

    private As400ConnectorConfig config;
    private final AS400 as400;

    public As400RpcConnection(As400ConnectorConfig config) {
        super();
        this.config = config;
        this.as400 = new AS400(config.getHostName(), config.getUser(), config.getPassword());
    }

    @Override
    public void close() throws Exception {
        // TODO Auto-generated method stub

    }

    public Long getJournalEntries(Long offset, BlockingRecieverConsumer consumer, BlockingNoDataConsumer nodataConsumer) throws RpcException {
    	RpcException exception = null;
        Long nextOffset = offset;
        try {
            RJNE0100 rnj = new RJNE0100(config.getJournalLibrary(), config.getJournalFile());
            ServiceProgramCall spc = new ServiceProgramCall(as400);
            rnj.addRetrieveCriteria(RetrieveKey.ENTTYP, "*ALL");
            rnj.addRetrieveCriteria(RetrieveKey.RCVRNG, "*CURCHAIN");
            if (offset == null || offset == 0) {
                rnj.addRetrieveCriteria(RetrieveKey.FROMENT, "*FIRST");
            }
            else {
                rnj.addRetrieveCriteria(RetrieveKey.FROMENT, offset);
            }
            spc.setProgram("/QSYS.LIB/QJOURNAL.SRVPGM", rnj.getProgramParameters());
            spc.setProcedureName("QjoRetrieveJournalEntries");
            spc.setAlignOn16Bytes(true);

            boolean success = spc.run();
            System.out.println("call: " + success);
            if (success) {
                Receiver r = rnj.getReceiver();
                while (r.nextEntry()) {
                    // TODO try round inner loop?
                	try {
                		Long currentOffset = Long.valueOf(r.getSequenceNumber());
                		nextOffset = currentOffset + 1;
	                    String obj = r.getObject();
	                    String file = obj.substring(0, 10).trim();
	                    String lib = obj.substring(10, 20).trim();
	                    String member = obj.substring(20, 30).trim();
	                    TableId tableId = new TableId("", lib, file);
	
	                    consumer.accept(currentOffset, r, tableId, member);
                	} catch (Exception e) {
                		if (exception == null)
                			exception = new RpcException("Failed to process record", e);
                		else
                			exception.addSuppressed(e); // TODO
                	}
                }
            } else {
            	nodataConsumer.accept();
            }
        }
        catch (Exception e) {
            throw new RpcException("Failed to process record", e);
        }
        return nextOffset;
    }

    public DynamicRecordFormat getRecordFormat(TableId tableId, String member, As400DatabaseSchema schema) throws RpcException {
        try {
            String recordFileName = String.format("/QSYS.LIB/%s.LIB/%s.FILE/%s.MBR", tableId.schema(), tableId.table(), member);
            DynamicRecordFormat recordFormat = DynamicRecordFormat.getRecordFormat(recordFileName, as400);
            // TODO I think really we are meant to register the table to monitor in the snapshot startup
            if (schema.schemaFor(tableId) == null) {
                schema.addSchema(tableId, recordFormat);
            }
            return recordFormat;
        }
        catch (Exception e) {
            throw new RpcException("failed to fetch record format", e);
        }
    }

    public static interface BlockingRecieverConsumer {
        void accept(Long offset, Receiver r, TableId tableId, String member) throws RpcException, InterruptedException;
    }
    
    public static interface BlockingNoDataConsumer {
        void accept() throws InterruptedException;
    }

    public static class RpcException extends Exception {

        public RpcException(String message, Throwable cause) {
            super(message, cause);
        }

        public RpcException(String message) {
            super(message);
        }

    }

}
