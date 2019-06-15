/**
 * Created: 12.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Mono;

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

    /**
     * @see io.r2dbc.spi.Statement#add()
     */
    @Override
    public Statement add()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @see io.r2dbc.spi.Statement#execute()
     */
    @Override
    public Mono<JdbcResult> execute()
    {
        return Mono.fromCallable(() -> {
            getLogger().debug("execute statement");
            return getStatement().executeQuery();
        }).handle((resultSet, sink) -> {
            try
            {
                JdbcResult result = createResult(resultSet, 0);
                sink.next(result);

                sink.complete();
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).cast(JdbcResult.class);
    }

    /**
     * @see io.r2dbc.jdbc.AbstractJdbcStatement#returnGeneratedValues(java.lang.String[])
     */
    @Override
    public Statement returnGeneratedValues(final String...columns)
    {
        throw new UnsupportedOperationException();
    }
}
