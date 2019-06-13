/**
 * Created: 11.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
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
    private final Mono<java.sql.Connection> connection;

    /**
     * Erstellt ein neues {@link JdbcConnection} Object.
     *
     * @param connection {@link java.sql.Connection}
     */
    public JdbcConnection(final java.sql.Connection connection)
    {
        super();

        Objects.requireNonNull(connection, "connection must not be null");

        this.connection = Mono.just(connection);
    }

    /**
     * @see io.r2dbc.spi.Connection#beginTransaction()
     */
    @Override
    public Mono<Void> beginTransaction()
    {
        return this.connection.handle((connection, sink) -> {
            try
            {
                if (!connection.getAutoCommit())
                {
                    connection.setAutoCommit(false);
                }
                else
                {
                    getLogger().debug("Skipping begin transaction because already in one");
                }

                sink.next(Mono.empty());
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();

        // @formatter:off
//        return Mono.defer(() -> supply(() -> {
//            if (!this.connection.getAutoCommit())
//            {
//                this.connection.setAutoCommit(false);
//                return Mono.empty();
//            }
//
//            getLogger().debug("Skipping begin transaction because already in one");
//            return Mono.empty();
//        }).get())
//        .onErrorMap(SQLException.class, GenericR2dbcExceptionFactory::create)
//        .then();
        // @formatter:on
    }

    /**
     * @see io.r2dbc.spi.Connection#close()
     */
    @Override
    public Mono<Void> close()
    {
        return this.connection.handle((connection, sink) -> {
            try
            {
                if (!connection.isClosed())
                {
                    connection.close();
                }
                else
                {
                    getLogger().debug("Skipping closing connection because it is already closed");
                }

                sink.next(Mono.empty());
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
        return this.connection.handle((connection, sink) -> {
            try
            {
                if (!connection.getAutoCommit())
                {
                    connection.commit();
                }
                else
                {
                    getLogger().debug("Skipping commit transaction because no transaction in progress.");
                }

                sink.next(Mono.empty());
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
        return new JdbcBatch();
    }

    /**
     * @see io.r2dbc.spi.Connection#createSavepoint(java.lang.String)
     */
    @Override
    public Publisher<Void> createSavepoint(final String name)
    {
        // throw new UnsupportedOperationException("not implemented");
        return Mono.error(new SQLFeatureNotSupportedException());
    }

    /**
     * @see io.r2dbc.spi.Connection#createStatement(java.lang.String)
     */
    @Override
    public Statement createStatement(final String sql)
    {
        return new JdbcStatement();
    }

    /**
     * @return {@link Logger}
     */
    private Logger getLogger()
    {
        return LOGGER;
    }

    /**
     * @see io.r2dbc.spi.Connection#releaseSavepoint(java.lang.String)
     */
    @Override
    public Publisher<Void> releaseSavepoint(final String name)
    {
        return Mono.error(new SQLFeatureNotSupportedException());
    }

    /**
     * @see io.r2dbc.spi.Connection#rollbackTransaction()
     */
    @Override
    public Mono<Void> rollbackTransaction()
    {
        return this.connection.handle((connection, sink) -> {
            try
            {
                if (!connection.getAutoCommit())
                {
                    connection.rollback();
                }
                else
                {
                    getLogger().debug("Skipping rollback to savepoint because no transaction in progress.");
                }

                sink.next(Mono.empty());
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
    public Publisher<Void> rollbackTransactionToSavepoint(final String name)
    {
        return Mono.error(new SQLFeatureNotSupportedException());
    }

    /**
     * @see io.r2dbc.spi.Connection#setTransactionIsolationLevel(io.r2dbc.spi.IsolationLevel)
     */
    @Override
    public Publisher<Void> setTransactionIsolationLevel(final IsolationLevel isolationLevel)
    {
        Objects.requireNonNull(isolationLevel, "isolationLevel must not be null");

        return this.connection.handle((connection, sink) -> {
            try
            {
                switch (isolationLevel)
                {
                    case READ_COMMITTED:
                        connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);
                        break;

                    case READ_UNCOMMITTED:
                        connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED);
                        break;

                    case REPEATABLE_READ:
                        connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_REPEATABLE_READ);
                        break;

                    case SERIALIZABLE:
                        connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);
                        break;

                    default:
                        connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_NONE);
                        break;
                }

                sink.next(Mono.empty());
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
    }
}
