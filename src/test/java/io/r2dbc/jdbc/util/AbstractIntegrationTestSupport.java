// Created: 14.06.2019
package io.r2dbc.jdbc.util;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.core.JdbcOperations;

import io.r2dbc.jdbc.JdbcConnection;
import io.r2dbc.jdbc.JdbcConnectionFactory;
import io.r2dbc.jdbc.JdbcConnectionFactoryProvider;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.test.TestKit;

/**
 * Support class for integration tests.
 *
 * @author Thomas Freese
 */
public abstract class AbstractIntegrationTestSupport
{
    /**
     *
     */
    private static JdbcConnection connection;

    /**
     *
     */
    private static JdbcConnectionFactory connectionFactory;

    /**
     *
     */
    @RegisterExtension
    static final DBServerExtension SERVER = new DBServerExtension();

    /**
     *
     */
    @AfterAll
    static void afterAll()
    {
        if (connection != null)
        {
            TestKit.close(connection);
        }
    }

    /**
     *
     */
    @BeforeAll
    static void beforeAll()
    {
        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, SERVER.getDataSource()).build();

        connectionFactory = (JdbcConnectionFactory) ConnectionFactories.get(options);
        connection = connectionFactory.create().block();
    }

    /**
     * @return {@link JdbcConnection}
     */
    protected static JdbcConnection getConnection()
    {
        return connection;
    }

    /**
     * @return {@link JdbcOperations}
     */
    protected JdbcOperations getJdbcOperations()
    {
        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();

        if (jdbcOperations == null)
        {
            throw new IllegalStateException("JdbcOperations not yet initialized.");
        }

        return jdbcOperations;
    }
}
