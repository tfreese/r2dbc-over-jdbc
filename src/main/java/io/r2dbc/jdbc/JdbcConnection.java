// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.ValidationDepth;
import reactor.core.publisher.Mono;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public class JdbcConnection implements Connection
{
    /**
     *
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcConnection.class);

    /**
    *
    */
    private final Codecs codecs;

    /**
    *
    */
    private final java.sql.Connection connection;

    /**
     *
     */
    private final Mono<java.sql.Connection> connectionMono;

    /**
     *
     */
    private final Map<String, Savepoint> savePoints = new HashMap<>();

    /**
     * Erstellt ein neues {@link JdbcConnection} Object.
     *
     * @param connection {@link java.sql.Connection}
     * @param codecs {@link Codecs}
     */
    public JdbcConnection(final java.sql.Connection connection, final Codecs codecs)
    {
        super();

        this.connection = Objects.requireNonNull(connection, "connection must not be null");
        this.codecs = Objects.requireNonNull(codecs, "codecs must not be null");

        this.connectionMono = Mono.just(this.connection);
    }

    /**
     * @see io.r2dbc.spi.Connection#beginTransaction()
     */
    @Override
    public Mono<Void> beginTransaction()
    {
        return this.connectionMono.handle((con, sink) -> {
            try
            {
                if (con.getAutoCommit())
                {
                    getLogger().debug("begin transaction");

                    con.setAutoCommit(false);
                }
                else
                {
                    getLogger().debug("Skipping begin transaction because there is one in progress.");
                }

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
    }

    /**
     * @see io.r2dbc.spi.Connection#close()
     */
    @Override
    public Mono<Void> close()
    {
        return this.connectionMono.handle((con, sink) -> {
            try
            {
                if (!con.isClosed())
                {
                    getLogger().debug("close connection");

                    con.close();
                }
                else
                {
                    getLogger().debug("Skipping closing connection because it is already closed");
                }

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
    }

    /**
     * @see io.r2dbc.spi.Connection#commitTransaction()
     */
    @Override
    public Mono<Void> commitTransaction()
    {
        return this.connectionMono.handle((con, sink) -> {
            try
            {
                if (!con.getAutoCommit())
                {
                    getLogger().debug("commit transaction");

                    con.commit();
                    con.setAutoCommit(true);
                }
                else
                {
                    getLogger().debug("Skipping commit transaction because no transaction in progress.");
                }

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
    }

    /**
     * @see io.r2dbc.spi.Connection#createBatch()
     */
    @Override
    public Batch createBatch()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @see io.r2dbc.spi.Connection#createSavepoint(java.lang.String)
     */
    @Override
    public Mono<Void> createSavepoint(final String name)
    {
        if (name == null)
        {
            throw new IllegalArgumentException("name is null");
        }

        // Begin Transaction.
        try
        {
            this.connection.setAutoCommit(false);
        }
        catch (SQLException ex)
        {
            // Ignore
        }

        // return Mono.error(new SQLFeatureNotSupportedException()).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
        return this.connectionMono.handle((con, sink) -> {
            try
            {
                Objects.requireNonNull(name, "name must not be null");

                getLogger().debug("create savepoint: {}", name);

                Savepoint savepoint = con.setSavepoint(name);
                this.savePoints.put(name, savepoint);

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
    }

    /**
     * @see io.r2dbc.spi.Connection#createStatement(java.lang.String)
     */
    @Override
    public JdbcStatement createStatement(final String sql)
    {
        if (sql == null)
        {
            throw new IllegalArgumentException("sql is null");
        }

        return new JdbcStatement(this.connection, sql);
        // return this.connectionMono.handle((connection, sink) -> {
        // try
        // {
        // getLogger().debug("create statement");
        // sink.next(new JdbcStatement(connection, sql));
        //
        // sink.complete();
        // }
        // catch (Exception sex)
        // {
        // sink.error(sex);
        // }
        // }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).cast(JdbcStatement.class).block();
    }

    /**
     * @return {@link Logger}
     */
    private Logger getLogger()
    {
        return LOGGER;
    }

    /**
     * @see io.r2dbc.spi.Connection#getMetadata()
     */
    @Override
    public ConnectionMetadata getMetadata()
    {
        return this.connectionMono.handle((con, sink) -> {
            try
            {
                getLogger().debug("get Metadata");

                DatabaseMetaData databaseMetaData = con.getMetaData();

                ConnectionMetadata connectionMetadata = new JdbcConnectionMetadata(databaseMetaData);

                sink.next(connectionMetadata);
                sink.complete();
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).cast(ConnectionMetadata.class).block();
    }

    /**
     * @see io.r2dbc.spi.Connection#getTransactionIsolationLevel()
     */
    @Override
    public IsolationLevel getTransactionIsolationLevel()
    {
        try
        {
            getLogger().debug("get transaction isolationLevel");

            int transactionIsolation = this.connection.getTransactionIsolation();
            IsolationLevel isolationLevel = null;

            if (transactionIsolation == java.sql.Connection.TRANSACTION_READ_COMMITTED)
            {
                isolationLevel = IsolationLevel.READ_COMMITTED;
            }
            else if (transactionIsolation == java.sql.Connection.TRANSACTION_READ_UNCOMMITTED)
            {
                isolationLevel = IsolationLevel.READ_UNCOMMITTED;
            }
            else if (transactionIsolation == java.sql.Connection.TRANSACTION_REPEATABLE_READ)
            {
                isolationLevel = IsolationLevel.REPEATABLE_READ;
            }
            else if (transactionIsolation == java.sql.Connection.TRANSACTION_SERIALIZABLE)
            {
                isolationLevel = IsolationLevel.SERIALIZABLE;
            }

            return isolationLevel;
        }
        catch (SQLException sex)
        {
            throw JdbcR2dbcExceptionFactory.create(sex);
        }
    }

    /**
     * @see io.r2dbc.spi.Connection#isAutoCommit()
     */
    @Override
    public boolean isAutoCommit()
    {
        try
        {
            getLogger().debug("is autocommit");

            return this.connection.getAutoCommit();
        }
        catch (SQLException sex)
        {
            throw JdbcR2dbcExceptionFactory.create(sex);
        }

        // return this.connectionMono.handle((connection, sink) -> {
        // try
        // {
        // getLogger().debug("is autocommit");
        //
        // sink.next(connection.getAutoCommit());
        // sink.complete();
        // }
        // catch (SQLException sex)
        // {
        // sink.error(sex);
        // }
        // }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).cast(Boolean.class).block();
    }

    /**
     * @see io.r2dbc.spi.Connection#releaseSavepoint(java.lang.String)
     */
    @Override
    public Mono<Void> releaseSavepoint(final String name)
    {
        if (name == null)
        {
            throw new IllegalArgumentException("name is null");
        }

        // return Mono.error(new SQLFeatureNotSupportedException()).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
        return this.connectionMono.handle((con, sink) -> {
            try
            {
                Objects.requireNonNull(name, "name must not be null");

                getLogger().debug("release savepoint: {}", name);

                Savepoint savepoint = this.savePoints.remove(name);
                con.releaseSavepoint(savepoint);

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
    }

    /**
     * @see io.r2dbc.spi.Connection#rollbackTransaction()
     */
    @Override
    public Mono<Void> rollbackTransaction()
    {
        return this.connectionMono.handle((con, sink) -> {
            try
            {
                if (!con.getAutoCommit())
                {
                    getLogger().debug("rollback transaction");

                    con.rollback();
                }
                else
                {
                    getLogger().debug("Skipping rollback because no transaction in progress.");
                }

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
    }

    /**
     * @see io.r2dbc.spi.Connection#rollbackTransactionToSavepoint(java.lang.String)
     */
    @Override
    public Mono<Void> rollbackTransactionToSavepoint(final String name)
    {
        // return Mono.error(new SQLFeatureNotSupportedException()).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
        return this.connectionMono.handle((con, sink) -> {
            try
            {
                Objects.requireNonNull(name, "name must not be null");

                getLogger().debug("rollback transaction savepoint: {}", name);

                Savepoint savepoint = this.savePoints.remove(name);
                con.rollback(savepoint);

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
    }

    /**
     * @see io.r2dbc.spi.Connection#setAutoCommit(boolean)
     */
    @Override
    public Mono<Void> setAutoCommit(final boolean autoCommit)
    {
        return this.connectionMono.handle((con, sink) -> {
            try
            {
                getLogger().debug("autoCommit: {}", autoCommit);

                con.setAutoCommit(autoCommit);

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
    }

    /**
     * @see io.r2dbc.spi.Connection#setTransactionIsolationLevel(io.r2dbc.spi.IsolationLevel)
     */
    @Override
    public Mono<Void> setTransactionIsolationLevel(final IsolationLevel isolationLevel)
    {
        return this.connectionMono.handle((con, sink) -> {
            try
            {
                Objects.requireNonNull(isolationLevel, "isolationLevel must not be null");

                getLogger().debug("set transaction isolationLevel: {}", isolationLevel);

                if (IsolationLevel.READ_COMMITTED.equals(isolationLevel))
                {
                    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);
                }
                else if (IsolationLevel.READ_UNCOMMITTED.equals(isolationLevel))
                {
                    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED);
                }
                else if (IsolationLevel.REPEATABLE_READ.equals(isolationLevel))
                {
                    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_REPEATABLE_READ);
                }
                else if (IsolationLevel.SERIALIZABLE.equals(isolationLevel))
                {
                    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);
                }
                else
                {
                    con.setTransactionIsolation(java.sql.Connection.TRANSACTION_NONE);
                }

                sink.next(Mono.empty());
                sink.complete();
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
    }

    /**
     * @see io.r2dbc.spi.Connection#validate(io.r2dbc.spi.ValidationDepth)
     */
    @Override
    public Mono<Boolean> validate(final ValidationDepth depth)
    {
        return this.connectionMono.handle((con, sink) -> {
            try
            {
                getLogger().debug("validate");

                sink.next(!con.isClosed());

                sink.complete();
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).cast(Boolean.class);
    }
}
