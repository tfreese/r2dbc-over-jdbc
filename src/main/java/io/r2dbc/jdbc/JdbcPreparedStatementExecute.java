/**
 * Created: 12.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * R2DBC Adapter for JDBC.<br>
 * Only for SELECT Statements.
 *
 * @author Thomas Freese
 */
public class JdbcPreparedStatementExecute extends AbstractJdbcStatement
{
    /**
     * Erstellt ein neues {@link JdbcPreparedStatementExecute} Object.
     *
     * @param preparedStatement {@link PreparedStatement}
     */
    public JdbcPreparedStatementExecute(final PreparedStatement preparedStatement)
    {
        super(preparedStatement);
    }

    // /**
    // * @see io.r2dbc.spi.Statement#add()
    // */
    // @Override
    // public Statement add()
    // {
    // throw new UnsupportedOperationException();
    // }

    /**
     * @return {@link Mono}
     */
    @Override
    @SuppressWarnings("resource")
    protected Mono<Tuple2<ResultSet, int[]>> createExecuteMono()
    {
        return Mono.fromCallable(() -> {
            getLogger().debug("execute statement");

            // getBindings().prepareBatch(getStatement());

            int affectedRows = getStatement().executeUpdate();
            int[] rows = normalizeAffectedRowsForReactive(new int[affectedRows]);

            ResultSet resultSetGenKeys = getStatement().getGeneratedKeys();

            return Tuples.of(resultSetGenKeys, rows);
        });
    }

    /**
     * @see io.r2dbc.jdbc.AbstractJdbcStatement#createResult(java.sql.ResultSet, int[])
     */
    @Override
    protected JdbcResult createResult(final ResultSet resultSet, final int[] affectedRows) throws SQLException
    {
        return super.createResultAffectedRows(resultSet, affectedRows);
    }
}
