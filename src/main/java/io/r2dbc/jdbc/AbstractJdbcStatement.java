/**
 * Created: 14.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public abstract class AbstractJdbcStatement implements Statement
{
    /**
     *
     */
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     *
     */
    private final java.sql.PreparedStatement preparedStatement;

    /**
     * Erstellt ein neues {@link AbstractJdbcStatement} Object.
     *
     * @param preparedStatement {@link java.sql.PreparedStatement}
     */
    public AbstractJdbcStatement(final java.sql.PreparedStatement preparedStatement)
    {
        super();

        this.preparedStatement = Objects.requireNonNull(preparedStatement, "statement must not be null");
    }

    /**
     * @see io.r2dbc.spi.Statement#bind(int, java.lang.Object)
     */
    @Override
    public Statement bind(final int index, final Object value)
    {
        try
        {
            getStatement().setObject(index, value);
        }
        catch (SQLException sex)
        {
            throw JdbcR2dbcExceptionFactory.create(sex);
        }

        return this;
    }

    /**
     * @see io.r2dbc.spi.Statement#bind(java.lang.Object, java.lang.Object)
     */
    @Override
    public Statement bind(final Object identifier, final Object value)
    {
        if (identifier instanceof Integer)
        {
            return bind(((Integer) identifier).intValue(), value);
        }

        throw new UnsupportedOperationException();
    }

    /**
     * @see io.r2dbc.spi.Statement#bindNull(int, java.lang.Class)
     */
    @Override
    public Statement bindNull(final int index, final Class<?> type)
    {
        try
        {
            getStatement().setObject(index, null);
        }
        catch (SQLException sex)
        {
            throw JdbcR2dbcExceptionFactory.create(sex);
        }

        return this;
    }

    /**
     * @see io.r2dbc.spi.Statement#bindNull(java.lang.Object, java.lang.Class)
     */
    @Override
    public Statement bindNull(final Object identifier, final Class<?> type)
    {
        if (identifier instanceof Integer)
        {
            return bindNull(((Integer) identifier).intValue(), type);
        }

        throw new UnsupportedOperationException();
    }

    /**
     * @param resultSet {@link ResultSet}
     * @param rowsUpdated int
     * @return {@link JdbcResult}
     * @throws SQLException Falls was schief geht.
     */
    protected JdbcResult createResult(final ResultSet resultSet, final int rowsUpdated) throws SQLException
    {
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(resultSet);

        Flux<JdbcRow> rows = Flux.generate((final SynchronousSink<Map<Object, Object>> sink) -> {
            try
            {
                if (resultSet.next())
                {
                    Map<Object, Object> row = new HashMap<>();

                    int index = 0;

                    for (String columnName : rowMetadata.getColumnNames())
                    {
                        row.put(columnName, resultSet.getObject(columnName));
                        row.put(index, resultSet.getObject(columnName));
                        index++;
                    }

                    sink.next(row);
                }
                else
                {
                    getLogger().debug("close resultSet");
                    resultSet.close();

                    getLogger().debug("close statement");
                    getStatement().close();

                    sink.complete();
                }
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).map(row -> new JdbcRow(row)).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create);

        JdbcResult result = new JdbcResult(rows, Mono.just(rowMetadata), Mono.just(rowsUpdated));

        return result;
    }

    /**
     * @param resultSet {@link ResultSet}
     * @param affectedRows int[]
     * @return {@link JdbcResult}
     * @throws SQLException Falls was schief geht.
     */
    protected JdbcResult createResultAffectedRows(final ResultSet resultSet, final int[] affectedRows) throws SQLException
    {
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(resultSet);

        resultSet.close();

        AtomicInteger counter = new AtomicInteger(0);

        Flux<JdbcRow> rows = Flux.generate((final SynchronousSink<Map<Object, Object>> sink) -> {
            if (counter.get() < affectedRows.length)
            {
                Map<Object, Object> row = new HashMap<>();
                row.put(0, affectedRows[counter.get()]);

                counter.incrementAndGet();

                sink.next(row);
            }
            else
            {
                sink.complete();
            }
        }).map(row -> new JdbcRow(row));

        JdbcResult result = new JdbcResult(rows, Mono.just(rowMetadata), Mono.just(affectedRows.length));

        return result;
    }

    /**
     * @return {@link Logger}
     */
    protected Logger getLogger()
    {
        return this.logger;
    }

    /**
     * @return {@link java.sql.PreparedStatement}
     */
    protected java.sql.PreparedStatement getStatement()
    {
        return this.preparedStatement;
    }
}
