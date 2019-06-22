/**
 * Created: 12.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * R2DBC Adapter for JDBC.<br>
 * Only for SELECT Statements.
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

    // /**
    // * @see io.r2dbc.spi.Statement#add()
    // */
    // @Override
    // public Statement add()
    // {
    // throw new UnsupportedOperationException();
    // }

    /**
     * @see io.r2dbc.jdbc.AbstractJdbcStatement#createExecuteMono()
     */
    @SuppressWarnings("resource")
    @Override
    protected Mono<Tuple2<ResultSet, int[]>> createExecuteMono()
    {
        return Mono.fromCallable(() -> {
            getLogger().debug("execute statement");

            getBindings().prepareStatement(getStatement(), getBindings().getCurrent());

            ResultSet resultSet = getStatement().executeQuery();

            return Tuples.of(resultSet, new int[] {});
        });
    }
}
