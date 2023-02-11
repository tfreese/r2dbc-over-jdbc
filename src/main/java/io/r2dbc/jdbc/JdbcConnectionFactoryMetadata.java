// Created: 14.06.2019
package io.r2dbc.jdbc;

import io.r2dbc.spi.ConnectionFactoryMetadata;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public final class JdbcConnectionFactoryMetadata implements ConnectionFactoryMetadata {
    public static final String NAME = "jdbc";

    static final JdbcConnectionFactoryMetadata INSTANCE = new JdbcConnectionFactoryMetadata();

    @Override
    public String getName() {
        return NAME;
    }
}
