// Created: 14.06.2019
package io.r2dbc.jdbc;

import io.r2dbc.spi.ConnectionFactoryMetadata;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public final class JdbcConnectionFactoryMetadata implements ConnectionFactoryMetadata
{
    /**
     *
     */
    static final JdbcConnectionFactoryMetadata INSTANCE = new JdbcConnectionFactoryMetadata();
    /**
     *
     */
    public static final String NAME = "jdbc";

    /**
     * @see io.r2dbc.spi.ConnectionFactoryMetadata#getName()
     */
    @Override
    public String getName()
    {
        return NAME;
    }
}
