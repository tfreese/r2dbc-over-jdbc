// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public class JdbcConnection implements Connection {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcConnection.class);

    private final Codecs codecs;
    private final java.sql.Connection connection;
    private final Mono<java.sql.Connection> connectionMono;
    private final Map<String, Savepoint> savePoints = new HashMap<>();

    public JdbcConnection(final java.sql.Connection connection, final Codecs codecs) {
        super();

        this.connection = Objects.requireNonNull(connection, "connection must not be null");
        this.codecs = Objects.requireNonNull(codecs, "codecs must not be null");

        connectionMono = Mono.just(connection);
    }

    @Override
    public Mono<Void> beginTransaction() {
        return beginTransaction(null);
    }

    @Override
    public Mono<Void> beginTransaction(final TransactionDefinition definition) {
        return connectionMono.handle((con, sink) -> {
            try {
                if (con.getAutoCommit()) {
                    getLogger().debug("begin transaction");

                    con.setAutoCommit(false);
                }
                else {
                    getLogger().debug("Skipping begin transaction because there is one in progress.");
                }

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex) {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::convert).then();
    }

    @Override
    public Mono<Void> close() {
        return connectionMono.handle((con, sink) -> {
            try {
                if (!con.isClosed()) {
                    getLogger().debug("close connection");

                    con.close();
                }
                else {
                    getLogger().debug("Skipping closing connection because it is already closed");
                }

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex) {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::convert).then();
    }

    @Override
    public Mono<Void> commitTransaction() {
        return connectionMono.handle((con, sink) -> {
            try {
                if (!con.getAutoCommit()) {
                    getLogger().debug("commit transaction");

                    con.commit();
                    con.setAutoCommit(true);
                }
                else {
                    getLogger().debug("Skipping commit transaction because no transaction in progress.");
                }

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex) {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::convert).then();
    }

    @Override
    public Batch createBatch() {
        return new JdbcBatch(this);
    }

    @Override
    public Mono<Void> createSavepoint(final String name) {
        if (name == null) {
            throw new IllegalArgumentException("name is null");
        }

        // // Begin Transaction.
        // try {
        // connection.setAutoCommit(false);
        // }
        // catch (SQLException ex) {
        // // Ignore
        // }

        // return Mono.error(new SQLFeatureNotSupportedException()).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
        return connectionMono.handle((con, sink) -> {
            try {
                Objects.requireNonNull(name, "name must not be null");

                getLogger().debug("create savepoint: {}", name);

                con.setAutoCommit(false);

                final Savepoint savepoint = con.setSavepoint(name);
                savePoints.put(name, savepoint);

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex) {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::convert).then();
    }

    @Override
    public Statement createStatement(final String sql) {
        if (sql == null) {
            throw new IllegalArgumentException("sql is null");
        }

        return new JdbcStatement(connection, sql, codecs);
        // return connectionMono.handle((connection, sink) -> {
        // try {
        // getLogger().debug("create statement");
        // sink.next(new JdbcStatement(connection, sql));
        //
        // sink.complete();
        // }
        // catch (Exception sex) {
        // sink.error(sex);
        // }
        // }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).cast(JdbcStatement.class).block();
    }

    @Override
    public ConnectionMetadata getMetadata() {
        return connectionMono.handle((con, sink) -> {
            try {
                getLogger().debug("get Metadata");

                final DatabaseMetaData databaseMetaData = con.getMetaData();

                final ConnectionMetadata connectionMetadata = new JdbcConnectionMetadata(databaseMetaData);

                sink.next(connectionMetadata);
                sink.complete();
            }
            catch (SQLException sex) {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::convert).cast(ConnectionMetadata.class).block();
    }

    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        try {
            getLogger().debug("get transaction isolationLevel");

            final int transactionIsolation = connection.getTransactionIsolation();
            IsolationLevel isolationLevel = null;

            if (transactionIsolation == java.sql.Connection.TRANSACTION_READ_COMMITTED) {
                isolationLevel = IsolationLevel.READ_COMMITTED;
            }
            else if (transactionIsolation == java.sql.Connection.TRANSACTION_READ_UNCOMMITTED) {
                isolationLevel = IsolationLevel.READ_UNCOMMITTED;
            }
            else if (transactionIsolation == java.sql.Connection.TRANSACTION_REPEATABLE_READ) {
                isolationLevel = IsolationLevel.REPEATABLE_READ;
            }
            else if (transactionIsolation == java.sql.Connection.TRANSACTION_SERIALIZABLE) {
                isolationLevel = IsolationLevel.SERIALIZABLE;
            }

            return isolationLevel;
        }
        catch (SQLException sex) {
            throw JdbcR2dbcExceptionFactory.convert(sex);
        }
    }

    @Override
    public boolean isAutoCommit() {
        try {
            getLogger().debug("is autocommit");

            return connection.getAutoCommit();
        }
        catch (SQLException sex) {
            throw JdbcR2dbcExceptionFactory.convert(sex);
        }

        // return connectionMono.handle((connection, sink) -> {
        // try {
        // getLogger().debug("is autocommit");
        //
        // sink.next(connection.getAutoCommit());
        // sink.complete();
        // }
        // catch (SQLException sex) {
        // sink.error(sex);
        // }
        // }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).cast(Boolean.class).block();
    }

    @Override
    public Mono<Void> releaseSavepoint(final String name) {
        if (name == null) {
            throw new IllegalArgumentException("name is null");
        }

        // return Mono.error(new SQLFeatureNotSupportedException()).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
        return connectionMono.handle((con, sink) -> {
            try {
                Objects.requireNonNull(name, "name must not be null");

                getLogger().debug("release savepoint: {}", name);

                final Savepoint savepoint = savePoints.remove(name);
                con.releaseSavepoint(savepoint);

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex) {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::convert).then();
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return connectionMono.handle((con, sink) -> {
            try {
                if (!con.getAutoCommit()) {
                    getLogger().debug("rollback transaction");

                    con.rollback();
                }
                else {
                    getLogger().debug("Skipping rollback because no transaction in progress.");
                }

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex) {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::convert).then();
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(final String name) {
        // return Mono.error(new SQLFeatureNotSupportedException()).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
        return connectionMono.handle((con, sink) -> {
            try {
                Objects.requireNonNull(name, "name must not be null");

                getLogger().debug("rollback transaction savepoint: {}", name);

                final Savepoint savepoint = savePoints.remove(name);
                con.rollback(savepoint);

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex) {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::convert).then();
    }

    @Override
    public Mono<Void> setAutoCommit(final boolean autoCommit) {
        return connectionMono.handle((con, sink) -> {
            try {
                getLogger().debug("autoCommit: {}", autoCommit);

                con.setAutoCommit(autoCommit);

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex) {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::convert).then();
    }

    @Override
    public Publisher<Void> setLockWaitTimeout(final Duration timeout) {
        return Mono.empty();
    }

    @Override
    public Publisher<Void> setStatementTimeout(final Duration timeout) {
        return Mono.empty();
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(final IsolationLevel isolationLevel) {
        return connectionMono.handle((con, sink) -> {
            try {
                Objects.requireNonNull(isolationLevel, "isolationLevel must not be null");

                getLogger().debug("set transaction isolationLevel: {}", isolationLevel);

                if (IsolationLevel.READ_COMMITTED.equals(isolationLevel)) {
                    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);
                }
                else if (IsolationLevel.READ_UNCOMMITTED.equals(isolationLevel)) {
                    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED);
                }
                else if (IsolationLevel.REPEATABLE_READ.equals(isolationLevel)) {
                    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_REPEATABLE_READ);
                }
                else if (IsolationLevel.SERIALIZABLE.equals(isolationLevel)) {
                    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);
                }
                else {
                    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_NONE);
                }

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex) {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::convert).then();
    }

    @Override
    public Mono<Boolean> validate(final ValidationDepth depth) {
        return connectionMono.handle((con, sink) -> {
            try {
                getLogger().debug("validate");

                sink.next(!con.isClosed());

                sink.complete();
            }
            catch (SQLException sex) {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::convert).cast(Boolean.class);
    }

    private Logger getLogger() {
        return LOGGER;
    }
}
