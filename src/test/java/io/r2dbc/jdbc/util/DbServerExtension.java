// Created: 14.06.2019
package io.r2dbc.jdbc.util;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;

/**
 * @author Thomas Freese
 */
public final class DbServerExtension implements BeforeAllCallback, BeforeTestExecutionCallback, AfterAllCallback, AfterTestExecutionCallback
{
    /**
     *
     */
    public static final AtomicInteger ATOMIC_INTEGER = new AtomicInteger(1);
    /**
     *
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DbServerExtension.class);
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
    public static void showMemory()
    {
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
        LOGGER.debug("{} - afterAll", this.databaseType);

        HikariPoolMXBean poolMXBean = this.dataSource.getHikariPoolMXBean();

        LOGGER.debug("{} - Connections: idle={}, active={}, waiting={}", this.databaseType, poolMXBean.getIdleConnections(), poolMXBean.getActiveConnections(),
                poolMXBean.getThreadsAwaitingConnection());

        LOGGER.debug("{} - close datasource", this.databaseType);
        this.dataSource.close();

        long startTime = getStoreForGlobal(context).get("start-time", long.class);
        long duration = System.currentTimeMillis() - startTime;

        LOGGER.debug("{} - All Tests took {} ms.", this.databaseType, duration);
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
                config.setJdbcUrl("jdbc:hsqldb:mem:" + ATOMIC_INTEGER.getAndIncrement() + ";create=true;shutdown=false");

                break;

            case H2:
                // ;DB_CLOSE_DELAY=-1 schliesst NICHT die DB nach Ende der letzten Connection
                // ;DB_CLOSE_ON_EXIT=FALSE:
                config.setDriverClassName("org.h2.Driver");
                config.setJdbcUrl("jdbc:h2:mem:" + ATOMIC_INTEGER.getAndIncrement() + ";create=true;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE");
                break;

            case DERBY:
                config.setDriverClassName("org.apache.derby.jdbc.EmbeddedDriver");
                config.setJdbcUrl("jdbc:derby:memory:" + ATOMIC_INTEGER.getAndIncrement() + ";create=true");
                break;

            default:
                throw new IllegalArgumentException("unsupported databaseType: " + this.databaseType);
        }

        config.setUsername("sa");
        config.setPassword("");
        config.setPoolName(getDatabaseType().name());
        config.setMaximumPoolSize(16);
        config.setConnectionTimeout(getSqlTimeout().toMillis());
        config.setAutoCommit(true); // Sonst funktioniert Derby nicht.

        this.dataSource = new HikariDataSource(config);

        // Initialisierung triggern.
        this.dataSource.getConnection().close();

        this.jdbcOperations = new JdbcTemplate(this.dataSource);

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
        //
        // EmbeddedDatabase database = new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.HSQL).setName("" +
        // TestSuiteJdbc.ATOMIC_INTEGER.getAndIncrement()).build();
        //
        // // SingleConnectionDataSource singleConnectionDataSource = new SingleConnectionDataSource();
        // // singleConnectionDataSource.setDriverClassName("org.generic.jdbc.JDBCDriver");
        // // singleConnectionDataSource.setUrl("jdbc:generic:mem:" + TestSuiteJdbc.ATOMIC_INTEGER.getAndIncrement());
        // // // singleConnectionDataSource.setUrl("jdbc:generic:file:db/generic/generic;create=false;shutdown=true");
        // // singleConnectionDataSource.setSuppressClose(true);
        // // singleConnectionDataSource.setAutoCommit(true);
        //
        // // DataSource dataSource = singleConnectionDataSource;
        // dataSource = database;
        //
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
