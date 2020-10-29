package io.debezium.connector.db2as400.command;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.FileAttributes;
import com.ibm.as400.access.ProgramParameter;
import com.ibm.as400.access.ServiceProgramCall;

import io.debezium.connector.db2as400.JournalPosition;


public class JournalInfoRetrieval {
	static final Logger log = LoggerFactory.getLogger(JournalInfoRetrieval.class);
	
	public static JournalPosition getCurrentPosition(AS400 as400, String schema, String journalFile) throws Exception {
		JournalInfo ji = JournalInfoRetrieval.getReceiver(as400, schema, journalFile);
		Integer offset = getOffset(as400, ji);
		return new JournalPosition(Long.valueOf(offset), ji.receiver, ji.schema);
	}
	
	static final Pattern JOURNAL_REGEX = Pattern.compile("\\/[^/]*\\/[^/]*\\/(.*).JRN");
	public static String getJournal(AS400 as400, String schema) throws IllegalStateException {
		try {
			FileAttributes fa = new FileAttributes(as400, String.format("/QSYS.LIB/%s.LIB", schema));
			System.out.println(fa.getJournalIdentifier());
			Matcher m = JOURNAL_REGEX.matcher(fa.getJournal());
			if (m.matches()) {
				return m.group(1);
			} else {
				System.out.println("no match");
			}
		} catch (Exception e) {
			throw new IllegalStateException("Journal not found", e);
		}
		throw new IllegalStateException("Journal not found");
	}

	
	/**
	 * @see https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/apis/QJORJRNI.htm
	 * @param as400
	 * @param schema
	 * @param journalFile
	 * @return
	 * @throws Exception
	 */
	public static JournalInfo getReceiver(AS400 as400, String schema, String journalFile) throws Exception {
		int rcvLen = 32768;
		String jrnLib = padRight(journalFile, 10) + padRight(schema, 10);
		String format = "RJRN0200";
		ProgramParameter[] parameters = new ProgramParameter[] {
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, rcvLen),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Bin4().toBytes(rcvLen)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Text(20).toBytes(jrnLib)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Text(8).toBytes(format)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Text(0).toBytes("")),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Bin4().toBytes(0))
		};

		return callServiceProgram(as400, "/QSYS.LIB/QJOURNAL.SRVPGM", "QjoRetrieveJournalInformation", parameters, (byte[] data) -> {
			String journalReceiver = decodeString(data, 200, 10);
			String journalLib = decodeString(data, 210, 10);
			return new JournalInfo(journalReceiver, journalLib);
		});
	}
	
	/**
	 * @see https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/apis/QJORRCVI.htm
	 * @param as400
	 * @param journalInfo
	 * @return
	 * @throws Exception
	 */
	private static Integer getOffset(AS400 as400, JournalInfo journalInfo)
			throws Exception {
		int rcvLen = 32768;
		String jrnLib = padRight(journalInfo.receiver, 10) + padRight(journalInfo.schema, 10);
		String format = "RRCV0100";
		ProgramParameter[] parameters = new ProgramParameter[] {
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, rcvLen),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Bin4().toBytes(rcvLen)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Text(20).toBytes(jrnLib)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Text(8).toBytes(format)),
				new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new AS400Bin4().toBytes(0)) };

		return callServiceProgram(as400, "/QSYS.LIB/QJOURNAL.SRVPGM", "QjoRtvJrnReceiverInformation", parameters, (byte[] data) -> {
			String journalName = decodeString(data, 8, 10);
//			Integer firstSequence = decodeInt(data, 72);
			Integer lastSequence = decodeInt(data, 80);
			if (!journalName.equals(journalInfo.receiver)) {
				String msg = String.format("journal names don't match requested %s got %s", journalInfo.receiver, journalName);
				throw new Exception(msg);
			}
			return lastSequence;
		});
	}
	
	/**
	 * 
	 * @param <T> return type of processor
	 * @param as400
	 * @param programLibrary
	 * @param program
	 * @param parameters assumes first parameter is output
	 * @param processor
	 * @return output of processor
	 * @throws Exception 
	 */
	private static <T> T callServiceProgram(AS400 as400, String programLibrary, String program, ProgramParameter[] parameters, ProcessData<T> processor) throws Exception {
		ServiceProgramCall spc = new ServiceProgramCall(as400);

		spc.setProgram(programLibrary, parameters);
		spc.setProcedureName(program);
		spc.setAlignOn16Bytes(true);
		spc.setReturnValueFormat(ServiceProgramCall.NO_RETURN_VALUE);
		boolean success = spc.run();
		if (success) {
			return processor.process(parameters[0].getOutputData());
		} else {
			String msg = "call failed " + Arrays.asList(spc.getMessageList()).stream().map(x -> x.getText()).reduce("", (a, s) -> a + s);
			log.error(msg);
			throw new Exception(msg);
		}
	}
	
	public static Integer decodeInt(byte[] data, int offset) {
		byte [] b = Arrays.copyOfRange(data, offset, offset+4); 
		return (Integer)new AS400Bin4().toObject(b);		
	}
	
	public static String decodeString(byte[] data, int offset, int length) {
		byte[] b = Arrays.copyOfRange(data, offset, offset+length);
		return (String) new AS400Text(length).toObject(b);
	}
	
	public static String padRight(String s, int n) {
		return String.format("%1$-" + n + "s", s);
	}
	
	public static class JournalInfo {
		public String receiver;
		public String schema;

		public JournalInfo(String receiver, String lib) {
			super();
			this.receiver = receiver;
			this.schema = lib;
		}
	}	
	
	private interface ProcessData<T>  {
		public T process(byte[] data) throws Exception;
	}
}
