// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.util.Objects;

import javax.sql.DataSource;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public final class JdbcConnectionFactoryProvider implements ConnectionFactoryProvider
{
    /**
     * {@link DataSource}
     */
    public static final Option<Codecs> CODECS = Option.valueOf("codecs");

    /**
     * {@link DataSource}
     */
    public static final Option<DataSource> DATASOURCE = Option.valueOf("datasource");

    /**
     * @see io.r2dbc.spi.ConnectionFactoryProvider#create(io.r2dbc.spi.ConnectionFactoryOptions)
     */
    @Override
    public ConnectionFactory create(final ConnectionFactoryOptions connectionFactoryOptions)
    {
        Objects.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        // @formatter:off
        JdbcConnectionConfiguration.Builder builder = JdbcConnectionConfiguration.builder()
                .dataSource(connectionFactoryOptions.getValue(DATASOURCE))
                .codecs(connectionFactoryOptions.getValue(CODECS));
        // @formatter:on

        return new JdbcConnectionFactory(builder.build());
    }

    /**
     * @see io.r2dbc.spi.ConnectionFactoryProvider#getDriver()
     */
    @Override
    public String getDriver()
    {
        return "generic";
    }

    /**
     * @see io.r2dbc.spi.ConnectionFactoryProvider#supports(io.r2dbc.spi.ConnectionFactoryOptions)
     */
    @Override
    public boolean supports(final ConnectionFactoryOptions connectionFactoryOptions)
    {
        Objects.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        DataSource dataSource = connectionFactoryOptions.getValue(DATASOURCE);

        if (dataSource != null)
        {
            return true;
        }

        return false;
    }
}
