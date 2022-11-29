// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Objects;

import io.r2dbc.spi.ConnectionMetadata;

/**
 * @author Thomas Freese
 */
public class JdbcConnectionMetadata implements ConnectionMetadata
{
    private final String productName;

    private final String version;

    public JdbcConnectionMetadata(final DatabaseMetaData databaseMetaData) throws SQLException
    {
        super();

        Objects.requireNonNull(databaseMetaData, "databaseMetaData required");

        this.productName = databaseMetaData.getDatabaseProductName();
        this.version = databaseMetaData.getDatabaseProductVersion();
    }

    public JdbcConnectionMetadata(final String productName, final String version)
    {
        super();

        this.productName = Objects.requireNonNull(productName, "productName required");
        this.version = Objects.requireNonNull(version, "version required");
    }

    @Override
    public String getDatabaseProductName()
    {
        return this.productName;
    }

    @Override
    public String getDatabaseVersion()
    {
        return this.version;
    }
}
