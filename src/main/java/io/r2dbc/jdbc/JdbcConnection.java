/**
 * Created: 11.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Savepoint;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
     *
     */
    private final Map<String, Savepoint> savePoints = new HashMap<>();

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
                if (connection.getAutoCommit())
                {
                    getLogger().debug("begin transaction");

                    connection.setAutoCommit(false);
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
                    getLogger().debug("close connection");

                    connection.close();
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
        return this.connection.handle((connection, sink) -> {
            try
            {
                if (!connection.getAutoCommit())
                {
                    getLogger().debug("commit transaction");

                    connection.commit();
                    connection.setAutoCommit(true);
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
        // return Mono.error(new SQLFeatureNotSupportedException()).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
        return this.connection.handle((connection, sink) -> {
            try
            {
                Objects.requireNonNull(name, "name must not be null");

                getLogger().debug("create savepoint");

                Savepoint savepoint = connection.setSavepoint(name);
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
    public Statement createStatement(final String sql)
    {
        return this.connection.handle((connection, sink) -> {
            try
            {
                getLogger().debug("create statement");

                @SuppressWarnings("resource")
                PreparedStatement preparedStatement = connection.prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS);

                String loweredSql = sql.toLowerCase();

                if (loweredSql.startsWith("select"))
                {
                    sink.next(new JdbcPreparedStatementSelect(preparedStatement));
                }
                else if (loweredSql.startsWith("delete"))
                {
                    sink.next(new JdbcPreparedStatementDelete(preparedStatement));
                }
                else if (loweredSql.startsWith("update"))
                {
                    sink.next(new JdbcPreparedStatementUpdate(preparedStatement));
                }
                else if (loweredSql.startsWith("insert"))
                {
                    sink.next(new JdbcPreparedStatementInsert(preparedStatement));
                }
                else
                {
                    sink.error(new SQLSyntaxErrorException("unknown SQL operation"));
                }

                sink.complete();
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).cast(Statement.class).block();
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
    public Mono<Void> releaseSavepoint(final String name)
    {
        // return Mono.error(new SQLFeatureNotSupportedException()).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
        return this.connection.handle((connection, sink) -> {
            try
            {
                Objects.requireNonNull(name, "name must not be null");

                getLogger().debug("release savepoint");

                Savepoint savepoint = this.savePoints.remove(name);
                connection.releaseSavepoint(savepoint);

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
        return this.connection.handle((connection, sink) -> {
            try
            {
                if (!connection.getAutoCommit())
                {
                    getLogger().debug("rollback transaction");

                    connection.rollback();
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
        return this.connection.handle((connection, sink) -> {
            try
            {
                Objects.requireNonNull(name, "name must not be null");

                getLogger().debug("rollback transaction savepoint");

                Savepoint savepoint = this.savePoints.remove(name);
                connection.rollback(savepoint);

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
        return this.connection.handle((connection, sink) -> {
            try
            {
                Objects.requireNonNull(isolationLevel, "isolationLevel must not be null");

                getLogger().debug("transaction isolationLevel");

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
                sink.complete();
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).then();
    }
}
