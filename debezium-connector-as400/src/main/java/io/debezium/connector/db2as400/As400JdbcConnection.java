/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400JDBCDriver;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

public class As400JdbcConnection extends JdbcConnection {
    private static final Logger log = LoggerFactory.getLogger(As400JdbcConnection.class);
    private static final String URL_PATTERN = "jdbc:as400://${" + JdbcConfiguration.HOSTNAME + "}/${" + JdbcConfiguration.DATABASE + "}";

    private static final String GET_DATABASE_NAME = "SELECT CURRENT_SERVER FROM SYSIBM.SYSDUMMY1";

    private final String realDatabaseName;

    // private static final String GET_LIST_OF_KEY_COLUMNS = "SELECT c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.LENGTH, c.numeric_scale, c.numeric_precision,column_default"
    // + "FROM qsys2.SYSCOLUMNS c "
    // + "WHERE ((c.system_TABLE_NAME='STOCK' AND c.system_TABLE_SCHEMA='F63HLDDBRD') OR (c.TABLE_NAME='STOCK' AND c.TABLE_SCHEMA='F63HLDDBRD')) "
    // + "order by c.COLUMN_NAME";

    private static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
            AS400JDBCDriver.class.getName(),
            As400JdbcConnection.class.getClassLoader());

    public As400JdbcConnection(Configuration config) {
        super(config, FACTORY);
        realDatabaseName = retrieveRealDatabaseName();
        log.debug("connection:" + this.connectionString(URL_PATTERN));
    }

    public static As400JdbcConnection forTestDatabase(String databaseName) {
        return new As400JdbcConnection(JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDatabase(databaseName)
                .build());
    }

    public String getRealDatabaseName() {
        return realDatabaseName;
    }

    private String retrieveRealDatabaseName() {
        try {
            return queryAndMap(
                    GET_DATABASE_NAME,
                    singleResultMapper(rs -> rs.getString(1), "Could not retrieve database name"));
        }
        catch (SQLException e) {
            throw new RuntimeException("Couldn't obtain database name", e);
        }
    }
}
