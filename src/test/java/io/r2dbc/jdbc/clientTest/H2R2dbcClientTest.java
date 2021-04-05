// Created: 05.04.2021
package io.r2dbc.jdbc.clientTest;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import io.r2dbc.client.R2dbc;
import io.r2dbc.jdbc.JdbcConnectionFactoryProvider;
import io.r2dbc.jdbc.util.DBServerExtension;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;

/**
 * @author Thomas Freese
 */
public class H2R2dbcClientTest implements R2dbcClientTest
{
    /**
    *
    */
    @RegisterExtension
    static final DBServerExtension SERVER = new DBServerExtension(EmbeddedDatabaseType.H2);

    /**
     *
     */
    private final R2dbc r2dbc = new R2dbc(
            ConnectionFactories.get(ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, SERVER.getDataSource()).build()));

    /**
     * @see io.r2dbc.jdbc.clientTest.R2dbcClientTest#getJdbcOperations()
     */
    @Override
    public JdbcOperations getJdbcOperations()
    {
        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();

        if (jdbcOperations == null)
        {
            throw new IllegalStateException("JdbcOperations not yet initialized.");
        }

        return jdbcOperations;
    }

    /**
     * @see io.r2dbc.jdbc.clientTest.R2dbcClientTest#getR2dbcClient()
     */
    @Override
    public R2dbc getR2dbcClient()
    {
        return this.r2dbc;
    }
}
