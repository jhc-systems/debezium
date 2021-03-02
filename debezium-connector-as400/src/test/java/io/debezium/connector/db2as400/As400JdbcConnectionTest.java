/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;

public class As400JdbcConnectionTest {

    // @Mock
    Configuration config;

    private As400JdbcConnection createTestSubject() throws IOException {
        // when(config.getString("")).thenReturn("");
        Properties properties = new Properties();
        InputStream in = this.getClass().getClassLoader().getResourceAsStream("testDb.properties");
        properties.load(in);
        in.close();
        properties.put("table.include.list",
                "F63QUALDB7.AKBAL,F63QUALDB7.ASSET_RISK_SCORE,F63QUALDB7.BARGIN,F63QUALDB7.BENCH,F63QUALDB7.SECURITY_TYPE_GROUP,F63QUALDB7.CLIENT,F63QUALDB7.CLIEXT,F63QUALDB7.CLITYP,F63QUALDB7.COMDES,F63QUALDB7.CONTCT,F63QUALDB7.COUNTRY,F63QUALDB7.CURRENCY,F63QUALDB7.DEFAULTS,F63QUALDB7.DEFCFG,F63QUALDB7.EARNER,F63QUALDB7.EXTADD,F63QUALDB7.FINANCIAL_ACCOUNT,F63QUALDB7.FXCODE,F63QUALDB7.HOLD,F63QUALDB7.INVESTMENT_OBJECTIVE,F63QUALDB7.KYCLNT,F63QUALDB7.MXFRAMED,F63QUALDB7.ORDER,F63QUALDB7.PERCIX,F63QUALDB7.PERSON,F63QUALDB7.PORACC,F63QUALDB7.PORMOD,F63QUALDB7.PORREL,F63QUALDB7.PORTFOLIO_REVIEW,F63QUALDB7.PORTFOLIO,F63QUALDB7.PORTFOLIO_TYPE,F63QUALDB7.PRDCLX,F63QUALDB7.PRDPRD,F63QUALDB7.PRODUC,F63QUALDB7.RELATION,F63QUALDB7.RESERVED_HOLDING,F63QUALDB7.RESERVED_HOLDING_TYPE,F63QUALDB7.SECURITY_ADDITIONAL_DETAIL,F63QUALDB7.SECURITY_DETAIL_S_SECTION,F63QUALDB7.SECURITY_DESCRIPTION_BY_COUNTRY,F63QUALDB7.SECURITY_OVERRIDE,F63QUALDB7.SECURITY,F63QUALDB7.STOCK_CLASSIFICATION_DETAIL,F63QUALDB7.VALUATION_FORMAT");
        config = Configuration.from(properties);

        return new As400JdbcConnection(config);
    }

    @Test
    public void testReadSchema() throws Exception { // very slow test if schema is big
        As400JdbcConnection testSubject = createTestSubject();
        String schemaNamePattern = "F63QUALDB7";
        TableFilter tableFilter = new As400ConnectorConfig(config).getTableFilters().dataCollectionFilter();

        Tables tables = new Tables();
        String databaseCatalog = null;
        // TableFilter.fromPredicate(x -> { return true; });
        ColumnNameFilter columnFilter = null;
        boolean removeTablesNotFoundInJdbc = false;

        // default test
        testSubject.readSchema(tables, testSubject.database(), schemaNamePattern, tableFilter, columnFilter,
                removeTablesNotFoundInJdbc);
        // System.out.println(tables);
        assertThat(tables.size(), equalTo(44));
    }

    // todo test getJournalEntries with the 3 scenarios
    // 1 no entries and no continuation
    // 2 previous= false; one entry and continuation
    // 3 previous = false, one entry and nocontinuation
}
