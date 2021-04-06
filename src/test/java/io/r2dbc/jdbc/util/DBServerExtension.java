// Created: 14.06.2019
package io.r2dbc.jdbc.util;

import java.time.Duration;
import java.util.Objects;

import javax.sql.DataSource;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;

/**
 * @author Thomas Freese
 */
public final class DBServerExtension implements BeforeAllCallback, AfterAllCallback
{
    /**
     *
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DBServerExtension.class);

    /**
     *
     */
    private static final Duration SQL_TIMEOUT = Duration.ofSeconds(5);

    /**
     * @return {@link Duration}
     */
    public static Duration getSqlTimeout()
    {
        return SQL_TIMEOUT;
    }

    /**
     *
     */
    private final EmbeddedDatabaseType databaseType;

    /**
     *
     */
    private HikariDataSource dataSource;

    /**
     *
     */
    private JdbcOperations jdbcOperations;

    /**
     * Die Junit-{@link Extension} braucht zwingend einen Default-Constructor !
     */
    public DBServerExtension()
    {
        this(EmbeddedDatabaseType.HSQL);
    }

    /**
     * Erstellt ein neues {@link DBServerExtension} Object.
     *
     * @param databaseType {@link EmbeddedDatabaseType}
     */
    public DBServerExtension(final EmbeddedDatabaseType databaseType)
    {
        super();

        this.databaseType = Objects.requireNonNull(databaseType, "databaseType required");
    }

    /**
     * @see org.junit.jupiter.api.extension.AfterAllCallback#afterAll(org.junit.jupiter.api.extension.ExtensionContext)
     */
    @Override
    public void afterAll(final ExtensionContext context) throws Exception
    {
        HikariPoolMXBean poolMXBean = this.dataSource.getHikariPoolMXBean();

        LOGGER.debug("{} - Connections: idle={}, active={}, waiting={}", getDatabaseType(), poolMXBean.getIdleConnections(), poolMXBean.getActiveConnections(),
                poolMXBean.getThreadsAwaitingConnection());

        this.dataSource.close();
    }

    /**
     * @see org.junit.jupiter.api.extension.BeforeAllCallback#beforeAll(org.junit.jupiter.api.extension.ExtensionContext)
     */
    @Override
    public void beforeAll(final ExtensionContext context) throws Exception
    {
        // Class.forName(getDriver(), true, getClass().getClassLoader());

        // @formatter:off
//        this.dataSource = DataSourceBuilder.create().type(HikariDataSource.class)
//                .driverClassName("org.hsqldb.jdbc.JDBCDriver")
//                .url("jdbc:hsqldb:mem:%d"+ System.nanoTime())
//                .username("sa")
//                .password("")
//                .build()
//                ;
        // @formatter:on

        // ;MVCC=true;LOCK_MODE=0

        this.dataSource = new HikariDataSource();

        String databaseNameAndParams = System.nanoTime() + ";create=true";

        switch (getDatabaseType())
        {
            case HSQL:
                this.dataSource.setDriverClassName("org.hsqldb.jdbc.JDBCDriver");
                this.dataSource.setJdbcUrl("jdbc:hsqldb:mem:" + databaseNameAndParams);

                break;

            case H2:
                this.dataSource.setDriverClassName("org.h2.Driver");
                this.dataSource.setJdbcUrl("jdbc:h2:mem:" + databaseNameAndParams);
                break;

            case DERBY:
                this.dataSource.setDriverClassName("org.apache.derby.jdbc.EmbeddedDriver");
                this.dataSource.setJdbcUrl("jdbc:derby:memory:" + databaseNameAndParams);
                break;

            default:
                throw new IllegalArgumentException("unsupported databaseType: " + this.databaseType);
        }

        this.dataSource.setUsername("sa");
        this.dataSource.setPassword("");
        this.dataSource.setPoolName(getDatabaseType().name());

        this.dataSource.setMaximumPoolSize(10);
        // this.dataSource.setConnectionTimeout(TimeUnit.MILLISECONDS.toMillis(500));

        // Initialisierung triggern.
        this.dataSource.getConnection().close();

        this.jdbcOperations = new JdbcTemplate(this.dataSource);
    }

    /**
     * @return {@link EmbeddedDatabaseType}
     */
    public EmbeddedDatabaseType getDatabaseType()
    {
        return this.databaseType;
    }

    /**
     * @return {@link DataSource}
     */
    public DataSource getDataSource()
    {
        return this.dataSource;
    }

    /**
     * @return {@link JdbcOperations}
     */
    public JdbcOperations getJdbcOperations()
    {
        return this.jdbcOperations;
    }

    /**
     * Test-Übergreifender Object-Store.
     *
     * @param context {@link ExtensionContext}
     * @return {@link Store}
     */
    Store getStoreForClass(final ExtensionContext context)
    {
        return context.getStore(Namespace.create(getClass()));
    }

    /**
     * Test-Übergreifender Object-Store.
     *
     * @param context {@link ExtensionContext}
     * @return {@link Store}
     */
    Store getStoreForGlobal(final ExtensionContext context)
    {
        return context.getStore(Namespace.create("global"));
    }

    /**
     * Test-Übergreifender Object-Store.
     *
     * @param context {@link ExtensionContext}
     * @return {@link Store}
     */
    Store getStoreForMethod(final ExtensionContext context)
    {
        return context.getStore(Namespace.create(getClass(), context.getRequiredTestMethod()));
    }

    /**
     * @return String
     */
    public String getUrl()
    {
        return this.dataSource.getJdbcUrl();
    }

    /**
     * @return String
     */
    public String getUsername()
    {
        return this.dataSource.getUsername();
    }
}
