// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import io.r2dbc.jdbc.converter.Converters;
import io.r2dbc.jdbc.converter.sql.SqlMapper;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Result;
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
                getBindings().prepareStatement(stmt, getBindings().getLast());

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
     * @return {@link Result}
     * @throws SQLException Falls was schief geht.
     */
    protected Result createResult(final PreparedStatement stmt, final ResultSet resultSet, final int[] affectedRows) throws SQLException
    {
        JdbcRowMetadata rowMetadata = JdbcRowMetadata.of(resultSet);
        Iterable<ColumnMetadata> columnMetaDatas = rowMetadata.getColumnMetadatas();

        Flux<JdbcRow> rows = Flux.generate((final SynchronousSink<Map<Object, Object>> sink) -> {
            try
            {
                if ((resultSet != null) && resultSet.next())
                {
                    Map<Object, Object> row = new HashMap<>();

                    int index = 0;

                    for (ColumnMetadata columnMetaData : columnMetaDatas)
                    {
                        String columnLabel = columnMetaData.getName();
                        JDBCType jdbcType = (JDBCType) columnMetaData.getNativeTypeMetadata();

                        SqlMapper<?> mapper = Converters.getSqlMapper(jdbcType);
                        Object value = mapper.mapFromSql(resultSet, columnLabel);

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

        Result result = new JdbcResult(rows, Mono.just(rowMetadata), rowsUpdated);

        return result;
    }

    /**
     * @see io.r2dbc.spi.Statement#execute()
     */
    @SuppressWarnings("resource")
    @Override
    public Flux<Result> execute()
    {
        // @formatter:off
        return Flux.fromArray(getSql().split(";"))
                .map(String::trim)
                .flatMap(sql ->
                     createExecuteMono(getConnection(), sql).handle((context, sink) -> {
                        try
                        {
                            PreparedStatement stmt = context.getStmt();
                            ResultSet resultSet = context.getResultSet();
                            int[] affectedRows = context.getAffectedRows();

                            Result result = createResult(stmt, resultSet, affectedRows);

                            sink.next(result);

                            sink.complete();
                        }
                        catch (SQLException sex)
                        {
                            sink.error(sex);
                        }
                    }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).cast(JdbcResult.class)
        );
        // @formatter:on
    }
}
