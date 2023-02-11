// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.RowMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

/**
 * @author Thomas Freese
 */
public class JdbcStatement extends AbstractJdbcStatement {
    private static final int DEBUG_SQL_LENGTH = 80;

    public JdbcStatement(final java.sql.Connection connection, final String sql, final Codecs codecs) {
        super(connection, sql, codecs);
    }

    /**
     * @see io.r2dbc.spi.Statement#execute()
     */
    @Override
    public Flux<Result> execute() {
        getBindings().validateBinds();

        // @formatter:off
        return Flux.fromArray(getSql().split(";"))
                .map(String::strip)
                .flatMap(sql ->
                     createExecuteMono(getJdbcConnection(), sql).handle((context, sink) -> {
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
                    }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::convert).cast(JdbcResult.class)
        );
        // @formatter:on
    }

    protected Mono<Context> createExecuteMono(final java.sql.Connection connection, final String sql) {
        if (SqlOperation.SELECT.equals(getSqlOperation())) {
            return Mono.fromCallable(() -> {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("prepare statement: {}", prepareSqlForLog(sql));
                }

                PreparedStatement stmt = connection.prepareStatement(sql);
                getBindings().prepareStatement(stmt, getBindings().getLast());

                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("execute statement: {}", prepareSqlForLog(sql));
                }

                ResultSet resultSet = stmt.executeQuery();

                return new Context(stmt, resultSet, null);
            });
        }
        else if (SqlOperation.INSERT.equals(getSqlOperation())) {
            return Mono.fromCallable(() -> {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("prepare statement: {}", prepareSqlForLog(sql));
                }

                PreparedStatement stmt = connection.prepareStatement(sql, java.sql.Statement.RETURN_GENERATED_KEYS);
                getBindings().prepareBatch(stmt);

                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("execute statement: {}", prepareSqlForLog(sql));
                }

                int[] affectedRows = stmt.executeBatch();

                ResultSet resultSet = stmt.getGeneratedKeys();

                return new Context(stmt, resultSet, affectedRows);
            });
        }

        return Mono.fromCallable(() -> {
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("prepare statement: {}", prepareSqlForLog(sql));
            }

            PreparedStatement stmt = connection.prepareStatement(sql);
            getBindings().prepareBatch(stmt);

            if (getLogger().isDebugEnabled()) {
                getLogger().debug("execute statement: {}", prepareSqlForLog(sql));
            }

            int[] affectedRows = stmt.executeBatch();

            return new Context(stmt, null, affectedRows);
        });
    }

    /**
     * @param resultSet {@link ResultSet}, optional
     * @param affectedRows int[], optional
     */
    protected Result createResult(final PreparedStatement stmt, final ResultSet resultSet, final int[] affectedRows) throws SQLException {
        RowMetadata rowMetadata = JdbcRowMetadata.of(resultSet, getCodecs());

        Flux<JdbcRow> rows = Flux.generate((final SynchronousSink<JdbcRow> sink) -> {
            try {
                if ((resultSet != null) && resultSet.next()) {
                    Map<Integer, Object> row = new HashMap<>();

                    int index = 0;

                    for (ColumnMetadata columnMetaData : rowMetadata.getColumnMetadatas()) {
                        String columnLabel = columnMetaData.getName();
                        JDBCType jdbcType = (JDBCType) columnMetaData.getNativeTypeMetadata();

                        Object value = getCodecs().mapFromSql(jdbcType, resultSet, columnLabel);

                        row.put(index, value);
                        index++;
                    }

                    sink.next(new JdbcRow(rowMetadata, row, getCodecs()));
                }
                else {
                    if (resultSet != null) {
                        getLogger().debug("close resultSet");
                        resultSet.close();
                    }

                    getLogger().debug("close statement");
                    stmt.close();

                    sink.complete();
                }
            }
            catch (SQLException sex) {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::convert);

        //        Mono<Long> rowsUpdated = affectedRows != null ? Mono.just(IntStream.of(affectedRows).mapToLong(Long::valueOf).sum()) : Mono.empty();
        // Flux<Long> rowsUpdated = affectedRows != null ? Flux.fromStream(IntStream.of(affectedRows).mapToLong(Long::valueOf).boxed()) : Flux.empty();

        return new JdbcResult(rows, Mono.just(rowMetadata), affectedRows);
    }

    protected String prepareSqlForLog(final String sql) {
        if (sql.length() < DEBUG_SQL_LENGTH) {
            return sql;
        }

        return sql.substring(0, Math.min(sql.length(), DEBUG_SQL_LENGTH - 3)) + "...";
    }
}
