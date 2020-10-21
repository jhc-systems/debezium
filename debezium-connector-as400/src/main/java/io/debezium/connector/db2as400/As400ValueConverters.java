/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import io.debezium.jdbc.JdbcValueConverters;

/**
 * Conversion of DB2 specific datatypes.
 *
 * @author Jiri Pechanec, Peter Urbanetz
 *
 */
public class As400ValueConverters extends JdbcValueConverters {

    public As400ValueConverters() {
    }

}
