/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400.command;

import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.fnz.db2.journal.retrieve.JournalInfoRetrieval;
import com.fnz.db2.journal.retrieve.JournalInfoRetrieval.JournalLib;
import com.fnz.db2.journal.retrieve.JournalPosition;
import com.ibm.as400.access.AS400;

public class JournalInfoRetrievalTest {

    @Test
    public void test() throws Exception {
        AS400 as400 = new AS400("tracey", "msdev", "msdev");

        JournalPosition position = JournalInfoRetrieval.getCurrentPosition(as400, new JournalLib("QSQJRN", "MSDEVT"));
        assertThat(position, is(notNullValue()));
        assertThat(position.getJournalReciever(), not(emptyOrNullString()));
        assertThat(position.getSchema(), not(emptyOrNullString()));
        assertThat(position.getOffset(), is(greaterThanOrEqualTo(0L)));
    }

}
