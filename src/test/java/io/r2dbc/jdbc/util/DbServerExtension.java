// Created: 14.06.2019
package io.r2dbc.jdbc.util;

import java.sql.Connection;
import java.sql.Statement;
import java.text.NumberFormat;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
public final class DbServerExtension implements BeforeAllCallback, BeforeTestExecutionCallback, AfterAllCallback, AfterTestExecutionCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbServerExtension.class);
    private static final Duration SQL_TIMEOUT = Duration.ofSeconds(5);

    public static Duration getSqlTimeout() {
        return SQL_TIMEOUT;
    }

    public static void showMemory() {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }

        final Runtime runtime = Runtime.getRuntime();
        final long maxMemory = runtime.maxMemory();
        final long allocatedMemory = runtime.totalMemory();
        final long freeMemory = runtime.freeMemory();

        final long divider = 1024 * 1024;
        final String unit = "MB";

        final NumberFormat format = NumberFormat.getInstance();

        LOGGER.debug("Free memory: " + format.format(freeMemory / divider) + unit);
        LOGGER.debug("Allocated memory: " + format.format(allocatedMemory / divider) + unit);
        LOGGER.debug("Max memory: " + format.format(maxMemory / divider) + unit);
        LOGGER.debug("Total free memory: " + format.format((freeMemory + (maxMemory - allocatedMemory)) / divider) + unit);
    }

    private final EmbeddedDatabaseType databaseType;

    private HikariDataSource dataSource;
    private JdbcOperations jdbcOperations;

    /**
     * Die Junit-{@link Extension} braucht zwingend einen Default-Constructor !
     */
    public DbServerExtension() {
        this(EmbeddedDatabaseType.HSQL);
    }

    public DbServerExtension(final EmbeddedDatabaseType databaseType) {
        super();

        this.databaseType = Objects.requireNonNull(databaseType, "databaseType required");
    }

    @Override
    public void afterAll(final ExtensionContext context) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{} - afterAll", this.databaseType);

            final HikariPoolMXBean poolMXBean = this.dataSource.getHikariPoolMXBean();

            LOGGER.debug("{} - Connections: idle={}, active={}, waiting={}", this.databaseType, poolMXBean.getIdleConnections(), poolMXBean.getActiveConnections(),
                    poolMXBean.getThreadsAwaitingConnection());
        }

        LOGGER.debug("{} - close datasource", this.databaseType);

        switch (getDatabaseType()) {
            case HSQL:
            case H2:
                // Handled already by hsql with 'shutdown=true'.
                try (Connection connection = this.dataSource.getConnection();
                     Statement statement = connection.createStatement()) {
                    statement.execute("SHUTDOWN COMPACT");
                }
                catch (Exception ex) {
                    LOGGER.error(ex.getMessage());
                }

                break;

            case DERBY:
                break;

            default:
                throw new IllegalArgumentException("unsupported databaseType: " + this.databaseType);
        }

        this.dataSource.close();

        TimeUnit.MILLISECONDS.sleep(100);

        if (!this.dataSource.isClosed()) {
            this.dataSource.close();
        }

        if (LOGGER.isDebugEnabled()) {
            final long startTime = getStoreForGlobal(context).get("start-time", long.class);
            final long duration = System.currentTimeMillis() - startTime;

            LOGGER.debug("{} - All Tests took {} ms.", this.databaseType, duration);
        }

        this.dataSource = null;

        System.gc();
    }

    @Override
    public void afterTestExecution(final ExtensionContext context) throws Exception {
        // final Method testMethod = context.getRequiredTestMethod();
        // long startTime = getStoreForMethod(context).get("start-time", long.class);
        // long duration = System.currentTimeMillis() - startTime;
        //
        // LOGGER.debug("{} - Method [{}] took {} ms.", this.databaseType, testMethod.getName(), duration);
        // LOGGER.debug("{} - Idle Connections = {}", this.databaseType, this.dataSource.getHikariPoolMXBean().getIdleConnections());
    }

    @Override
    public void beforeAll(final ExtensionContext context) throws Exception {
        LOGGER.debug("{} - beforeAll", this.databaseType);

        getStoreForGlobal(context).put("start-time", System.currentTimeMillis());

        final HikariConfig config = new HikariConfig();

        switch (getDatabaseType()) {
            case HSQL -> {
                // ;shutdown=true schliesst die DB nach Ende der letzten Connection.
                // ;MVCC=true;LOCK_MODE=0
                config.setDriverClassName("org.hsqldb.jdbc.JDBCDriver");
                config.setJdbcUrl("jdbc:hsqldb:mem:" + UUID.randomUUID() + ";shutdown=true");
            }
            case H2 -> {
                // ;DB_CLOSE_DELAY=-1 schliesst NICHT die DB nach Ende der letzten Connection
                // ;DB_CLOSE_ON_EXIT=FALSE schliesst NICHT die DB nach Ende der Runtime
                config.setDriverClassName("org.h2.Driver");
                config.setJdbcUrl("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=0;DB_CLOSE_ON_EXIT=true");
            }
            case DERBY -> {
                config.setDriverClassName("org.apache.derby.iapi.jdbc.AutoloadedDriver");
                config.setJdbcUrl("jdbc:derby:memory:" + UUID.randomUUID() + ";create=true");
            }
            default -> throw new IllegalArgumentException("unsupported databaseType: " + this.databaseType);
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
        // this.dataSource.getConnection().close();

        this.jdbcOperations = new JdbcTemplate(this.dataSource);

        // Class.forName(getDriver(), true, getClass().getClassLoader());

        // ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        // populator.addScript(new ClassPathResource("hsqldb-schema.sql"));
        // populator.addScript(new ClassPathResource("hsqldb-data.sql"));
        // populator.execute(dataSource);
    }

    @Override
    public void beforeTestExecution(final ExtensionContext context) throws Exception {
        getStoreForMethod(context).put("start-time", System.currentTimeMillis());
    }

    public DataSource getDataSource() {
        return this.dataSource;
    }

    public EmbeddedDatabaseType getDatabaseType() {
        return this.databaseType;
    }

    public String getDriver() {
        return this.dataSource.getDriverClassName();
    }

    public JdbcOperations getJdbcOperations() {
        return this.jdbcOperations;
    }

    public String getPassword() {
        return this.dataSource.getPassword();
    }

    public String getUrl() {
        return this.dataSource.getJdbcUrl();
    }

    public String getUsername() {
        return this.dataSource.getUsername();
    }

    /**
     * Object-Store pro Test-Klasse.
     */
    Store getStoreForClass(final ExtensionContext context) {
        return context.getStore(Namespace.create(getClass()));
    }

    /**
     * Object-Store für den gesamten Test.
     */
    Store getStoreForGlobal(final ExtensionContext context) {
        return context.getStore(Namespace.create("global"));
    }

    /**
     * Object-Store pro Test-Methode.
     */
    Store getStoreForMethod(final ExtensionContext context) {
        return context.getStore(Namespace.create(getClass(), context.getRequiredTestMethod()));
    }
}
