/**
 * Created: 12.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public class JdbcPreparedStatementSelect extends AbstractJdbcStatement
{
    /**
     * Erstellt ein neues {@link JdbcPreparedStatementSelect} Object.
     *
     * @param preparedStatement {@link PreparedStatement}
     */
    public JdbcPreparedStatementSelect(final PreparedStatement preparedStatement)
    {
        super(preparedStatement);
    }

    /**
     * @see io.r2dbc.spi.Statement#add()
     */
    @Override
    public Statement add()
    {
        throw new UnsupportedOperationException();
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
     * @see io.r2dbc.spi.Statement#execute()
     */
    @Override
    public Mono<JdbcResult> execute()
    {
        return Mono.fromCallable(getStatement()::executeQuery).handle((resultSet, sink) -> {
            try
            {
                getLogger().debug("execute statement");

                JdbcRowMetadata rowMetadata = new JdbcRowMetadata(resultSet);

                Flux<JdbcRow> rows = Flux.generate((final SynchronousSink<Map<Object, Object>> ss) -> {
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

                            ss.next(row);
                        }
                        else
                        {
                            getLogger().debug("close resultSet");
                            resultSet.close();

                            getLogger().debug("close statement");
                            getStatement().close();

                            ss.complete();
                        }
                    }
                    catch (SQLException sex)
                    {
                        ss.error(sex);
                    }
                }).map(row -> new JdbcRow(row)).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create);

                JdbcResult result = new JdbcResult(rows, Mono.just(rowMetadata), Mono.just(0));
                sink.next(result);

                sink.complete();
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).cast(JdbcResult.class);
    }
}
