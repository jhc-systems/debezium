package io.debezium.connector.db2as400.command;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

import org.junit.Test;

import com.ibm.as400.access.AS400;

import io.debezium.connector.db2as400.JournalPosition;

public class JournalInfoRetrievalTest {

	@Test
	public void test() throws Exception {
		AS400 as400 = new AS400("tracey", "msdev", "msdev");
		
		JournalPosition position = JournalInfoRetrieval.getCurrentPosition(as400, "MSDEVT", "QSQJRN");
		assertThat(position, is(notNullValue()));
		assertThat(position.getJournalReciever(), not(emptyOrNullString()));
		assertThat(position.getJournalLib(), not(emptyOrNullString()));
		assertThat(position.getOffset(), is(greaterThanOrEqualTo(0L)));
	}

}
