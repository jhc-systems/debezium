/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400JDBCDriver;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

public class As400JdbcConnection extends JdbcConnection {
    private static final Logger log = LoggerFactory.getLogger(As400JdbcConnection.class);
    private static final String URL_PATTERN = "jdbc:as400://${" + JdbcConfiguration.HOSTNAME + "}/${" + JdbcConfiguration.DATABASE + "}";

    private static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
            AS400JDBCDriver.class.getName(),
            As400JdbcConnection.class.getClassLoader());

    public As400JdbcConnection(Configuration config) {
        super(config, FACTORY);
        log.debug("connection:" + this.connectionString(URL_PATTERN));
    }

    public static As400JdbcConnection forTestDatabase(String databaseName) {
        return new As400JdbcConnection(JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDatabase(databaseName)
                // .with("characterEncoding", "utf8")
                .build());
    }

}
