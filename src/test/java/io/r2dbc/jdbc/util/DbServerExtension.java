// Created: 14.06.2019
package io.r2dbc.jdbc.util;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

/**
 * @author Thomas Freese
 */
public final class DbServerExtension implements BeforeAllCallback, BeforeTestExecutionCallback, AfterAllCallback, AfterTestExecutionCallback
{
    /**
     *
     */
    private static final AtomicInteger ATOMIC_INTEGER = new AtomicInteger(1);
    /**
     *
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DbServerExtension.class);
    /**
     *
     */
    private static final Duration SQL_TIMEOUT = Duration.ofSeconds(5);

    /**
     * @return String
     */
    public static String createDbName()
    {
        String dbName = "db-" + ATOMIC_INTEGER.getAndIncrement();

        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("Create DB: {}", dbName);
        }

        return dbName;
    }

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
    public static void showMemory()
    {
        if (!LOGGER.isDebugEnabled())
        {
            return;
        }

        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long allocatedMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();

        long divider = 1024 * 1024;
        String unit = "MB";

        NumberFormat format = NumberFormat.getInstance();

        LOGGER.debug("Free memory: " + format.format(freeMemory / divider) + unit);
        LOGGER.debug("Allocated memory: " + format.format(allocatedMemory / divider) + unit);
        LOGGER.debug("Max memory: " + format.format(maxMemory / divider) + unit);
        LOGGER.debug("Total free memory: " + format.format((freeMemory + (maxMemory - allocatedMemory)) / divider) + unit);
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
    public DbServerExtension()
    {
        this(EmbeddedDatabaseType.HSQL);
    }

    /**
     * Erstellt ein neues {@link DbServerExtension} Object.
     *
     * @param databaseType {@link EmbeddedDatabaseType}
     */
    public DbServerExtension(final EmbeddedDatabaseType databaseType)
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
        if (LOGGER.isDebugEnabled())
        {
            LOGGER.debug("{} - afterAll", this.databaseType);

            HikariPoolMXBean poolMXBean = this.dataSource.getHikariPoolMXBean();

            LOGGER.debug("{} - Connections: idle={}, active={}, waiting={}", this.databaseType, poolMXBean.getIdleConnections(), poolMXBean.getActiveConnections(),
                    poolMXBean.getThreadsAwaitingConnection());
        }

        LOGGER.debug("{} - close datasource", this.databaseType);

        switch (getDatabaseType())
        {
            case HSQL:
            case H2:
//                try (Connection connection = this.dataSource.getConnection();
//                     Statement statement = connection.createStatement())
//                {
//                    statement.execute("SHUTDOWN");
//                }
//                catch (Exception ex)
//                {
//                    LOGGER.error(ex.getMessage());
//                }
                break;

            case DERBY:
                break;

            default:
                throw new IllegalArgumentException("unsupported databaseType: " + this.databaseType);
        }

        this.dataSource.close();

        TimeUnit.MILLISECONDS.sleep(100);

        if (!this.dataSource.isClosed())
        {
            this.dataSource.close();
        }

        if (LOGGER.isDebugEnabled())
        {
            long startTime = getStoreForGlobal(context).get("start-time", long.class);
            long duration = System.currentTimeMillis() - startTime;

            LOGGER.debug("{} - All Tests took {} ms.", this.databaseType, duration);
        }

        this.dataSource = null;

        System.gc();
    }

    /**
     * @see org.junit.jupiter.api.extension.AfterTestExecutionCallback#afterTestExecution(org.junit.jupiter.api.extension.ExtensionContext)
     */
    @Override
    public void afterTestExecution(final ExtensionContext context) throws Exception
    {
        // Method testMethod = context.getRequiredTestMethod();
        // long startTime = getStoreForMethod(context).get("start-time", long.class);
        // long duration = System.currentTimeMillis() - startTime;
        //
        // LOGGER.debug("{} - Method [{}] took {} ms.", this.databaseType, testMethod.getName(), duration);
        // LOGGER.debug("{} - Idle Connections = {}", this.databaseType, this.dataSource.getHikariPoolMXBean().getIdleConnections());
    }

    /**
     * @see org.junit.jupiter.api.extension.BeforeAllCallback#beforeAll(org.junit.jupiter.api.extension.ExtensionContext)
     */
    @Override
    public void beforeAll(final ExtensionContext context) throws Exception
    {
        LOGGER.debug("{} - beforeAll", this.databaseType);

        getStoreForGlobal(context).put("start-time", System.currentTimeMillis());

        HikariConfig config = new HikariConfig();

        switch (getDatabaseType())
        {
            case HSQL:
                // ;shutdown=true schliesst die DB nach Ende der letzten Connection.
                // ;MVCC=true;LOCK_MODE=0
                config.setDriverClassName("org.hsqldb.jdbc.JDBCDriver");
                config.setJdbcUrl("jdbc:hsqldb:mem:" + createDbName() + ";shutdown=true");

                break;

            case H2:
                // ;DB_CLOSE_DELAY=-1 schliesst NICHT die DB nach Ende der letzten Connection
                // ;DB_CLOSE_ON_EXIT=FALSE schliesst NICHT die DB nach Ende der Runtime
                config.setDriverClassName("org.h2.Driver");
                config.setJdbcUrl("jdbc:h2:mem:" + createDbName() + ";DB_CLOSE_DELAY=0;DB_CLOSE_ON_EXIT=true");
                break;

            case DERBY:
                config.setDriverClassName("org.apache.derby.jdbc.EmbeddedDriver");
                config.setJdbcUrl("jdbc:derby:memory:" + createDbName() + ";create=true");
                break;

            default:
                throw new IllegalArgumentException("unsupported databaseType: " + this.databaseType);
        }

        config.setUsername("sa");
        config.setPassword("");
        config.setPoolName(getDatabaseType().name());
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(16);
        config.setConnectionTimeout(getSqlTimeout().toMillis());
        config.setAutoCommit(true);

        this.dataSource = new HikariDataSource(config);

        // Initialisierung triggern.
        //this.dataSource.getConnection().close();

        this.jdbcOperations = new JdbcTemplate(this.dataSource);

        // Class.forName(getDriver(), true, getClass().getClassLoader());

        // ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        // populator.addScript(new ClassPathResource("hsqldb-schema.sql"));
        // populator.addScript(new ClassPathResource("hsqldb-data.sql"));
        // populator.execute(dataSource);
    }

    /**
     * @see org.junit.jupiter.api.extension.BeforeTestExecutionCallback#beforeTestExecution(org.junit.jupiter.api.extension.ExtensionContext)
     */
    @Override
    public void beforeTestExecution(final ExtensionContext context) throws Exception
    {
        getStoreForMethod(context).put("start-time", System.currentTimeMillis());
    }

    /**
     * @return {@link DataSource}
     */
    public DataSource getDataSource()
    {
        return this.dataSource;
    }

    /**
     * @return {@link EmbeddedDatabaseType}
     */
    public EmbeddedDatabaseType getDatabaseType()
    {
        return this.databaseType;
    }

    /**
     * @return String
     */
    public String getDriver()
    {
        return this.dataSource.getDriverClassName();
    }

    /**
     * @return {@link JdbcOperations}
     */
    public JdbcOperations getJdbcOperations()
    {
        return this.jdbcOperations;
    }

    /**
     * @return String
     */
    public String getPassword()
    {
        return this.dataSource.getPassword();
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

    /**
     * Object-Store pro Test-Klasse.
     *
     * @param context {@link ExtensionContext}
     *
     * @return {@link Store}
     */
    Store getStoreForClass(final ExtensionContext context)
    {
        return context.getStore(Namespace.create(getClass()));
    }

    /**
     * Object-Store für den gesamten Test.
     *
     * @param context {@link ExtensionContext}
     *
     * @return {@link Store}
     */
    Store getStoreForGlobal(final ExtensionContext context)
    {
        return context.getStore(Namespace.create("global"));
    }

    /**
     * Object-Store pro Test-Methode.
     *
     * @param context {@link ExtensionContext}
     *
     * @return {@link Store}
     */
    Store getStoreForMethod(final ExtensionContext context)
    {
        return context.getStore(Namespace.create(getClass(), context.getRequiredTestMethod()));
    }
}
