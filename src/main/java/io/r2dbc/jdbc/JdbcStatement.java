/**
 * Created: 25.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import io.r2dbc.jdbc.codec.Codecs;
import io.r2dbc.jdbc.codec.decoder.Decoder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

/**
 * @author Thomas Freese
 */
public class JdbcStatement extends AbstractJdbcStatement
{
    /**
     * Erstellt ein neues {@link JdbcStatement} Object.
     *
     * @param connection {@link java.sql.Connection}
     * @param sql String
     */
    public JdbcStatement(final java.sql.Connection connection, final String sql)
    {
        super(connection, sql);
    }

    /**
     * @param connection {@link java.sql.Connection}
     * @param sql String
     * @return {@link Mono}
     */
    @SuppressWarnings("resource")
    protected Mono<Context> createExecuteMono(final java.sql.Connection connection, final String sql)
    {
        final int DEBUG_SQL_LENGTH = 40;

        if (SQL_OPERATION.SELECT.equals(getSqlOperation()))
        {
            return Mono.fromCallable(() -> {
                if (getLogger().isDebugEnabled())
                {
                    getLogger().debug("prepare statement: {}...", sql.substring(0, Math.min(sql.length(), DEBUG_SQL_LENGTH)));
                }

                PreparedStatement stmt = connection.prepareStatement(sql);
                getBindings().prepareStatement(stmt, getBindings().getCurrent());

                if (getLogger().isDebugEnabled())
                {
                    getLogger().debug("execute statement: {}...", sql.substring(0, Math.min(sql.length(), DEBUG_SQL_LENGTH)));
                }

                ResultSet resultSet = stmt.executeQuery();

                return new Context(stmt, resultSet, null);
            });
        }
        else if (SQL_OPERATION.INSERT.equals(getSqlOperation()))
        {
            return Mono.fromCallable(() -> {
                if (getLogger().isDebugEnabled())
                {
                    getLogger().debug("prepare statement: {}...", sql.substring(0, Math.min(sql.length(), DEBUG_SQL_LENGTH)));
                }

                PreparedStatement stmt = connection.prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS);
                getBindings().prepareBatch(stmt);

                if (getLogger().isDebugEnabled())
                {
                    getLogger().debug("execute statement: {}...", sql.substring(0, Math.min(sql.length(), DEBUG_SQL_LENGTH)));
                }

                int[] affectedRows = stmt.executeBatch();

                ResultSet resultSet = stmt.getGeneratedKeys();

                return new Context(stmt, resultSet, affectedRows);
            });
        }

        return Mono.fromCallable(() -> {
            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("prepare statement: {}...", sql.substring(0, Math.min(sql.length(), DEBUG_SQL_LENGTH)));
            }

            PreparedStatement stmt = connection.prepareStatement(sql);
            getBindings().prepareBatch(stmt);

            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("execute statement: {}...", sql.substring(0, Math.min(sql.length(), DEBUG_SQL_LENGTH)));
            }

            int[] affectedRows = stmt.executeBatch();
            // affectedRows = normalizeAffectedRowsForReactive(affectedRows);

            return new Context(stmt, null, affectedRows);
        });
    }

    /**
     * @param stmt {@link PreparedStatement}
     * @param resultSet {@link ResultSet}, optional
     * @param affectedRows int[], optional
     * @return {@link JdbcResult}
     * @throws SQLException Falls was schief geht.
     */
    protected JdbcResult createResult(final PreparedStatement stmt, final ResultSet resultSet, final int[] affectedRows) throws SQLException
    {
        Mono<JdbcRowMetadata> rowMetadata = JdbcRowMetadata.of(resultSet);
        List<JdbcColumnMetadata> columnMetaDatas = rowMetadata.map(JdbcRowMetadata::getColumnMetadatas).block();

        Flux<JdbcRow> rows = Flux.generate((final SynchronousSink<Map<Object, Object>> sink) -> {
            try
            {
                if ((resultSet != null) && resultSet.next())
                {
                    Map<Object, Object> row = new HashMap<>();

                    int index = 0;

                    for (JdbcColumnMetadata columnMetaData : columnMetaDatas)
                    {
                        String columnLabel = columnMetaData.getName();
                        int sqlType = (int) columnMetaData.getNativeTypeMetadata();

                        Decoder<?> decoder = Codecs.getDecoder(sqlType);
                        Object value = decoder.decode(resultSet, columnLabel);

                        row.put(columnLabel, value);
                        row.put(index, value);
                        index++;
                    }

                    sink.next(row);
                }
                else
                {
                    if (resultSet != null)
                    {
                        getLogger().debug("close resultSet");
                        resultSet.close();
                    }

                    getLogger().debug("close statement");
                    stmt.close();

                    sink.complete();
                }
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).map(JdbcRow::new).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create);

        Mono<Integer> rowsUpdated = affectedRows != null ? Mono.just(IntStream.of(affectedRows).sum()) : Mono.empty();

        JdbcResult result = new JdbcResult(rows, rowMetadata, rowsUpdated);

        return result;
    }

    /**
     * @see io.r2dbc.spi.Statement#execute()
     */
    @SuppressWarnings("resource")
    @Override
    public Flux<JdbcResult> execute()
    {
        // @formatter:off
        return Flux.fromArray(getSql().split(";"))
                .map(String::trim)
                .flatMap(sql -> {
                    return createExecuteMono(getConnection(), sql).handle((context, sink) -> {
                        try
                        {
                            PreparedStatement stmt = context.getStmt();
                            ResultSet resultSet = context.getResultSet();
                            int[] affectedRows = context.getAffectedRows();

                            JdbcResult result = createResult(stmt, resultSet, affectedRows);

                            sink.next(result);

                            sink.complete();
                        }
                        catch (SQLException sex)
                        {
                            sink.error(sex);
                        }
                    }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).cast(JdbcResult.class);
        });
        // @formatter:on
    }
}
