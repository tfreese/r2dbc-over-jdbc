/**
 * Created: 12.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

/**
 * R2DBC Adapter for JDBC.<br>
 * Only for INSERT, UPDATE and DELETE Statements.
 *
 * @author Thomas Freese
 */
public class JdbcPreparedStatementUpdate extends AbstractJdbcStatement
{
    /**
    *
    */
    private boolean returnGeneratedValues = false;

    /**
     * Erstellt ein neues {@link JdbcPreparedStatementUpdate} Object.
     *
     * @param preparedStatement {@link PreparedStatement}
     */
    public JdbcPreparedStatementUpdate(final PreparedStatement preparedStatement)
    {
        super(preparedStatement);
    }

    /**
     * @see io.r2dbc.spi.Statement#add()
     */
    @Override
    public Statement add()
    {
        try
        {
            getStatement().addBatch();
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
    @SuppressWarnings("resource")
    @Override
    public Mono<JdbcResult> execute()
    {
        return Mono.fromCallable(() -> {
            getLogger().debug("execute statement");

            int[] affectedRows = getStatement().executeBatch();
            ResultSet resultSetGenKeys = getStatement().getGeneratedKeys();

            return Tuples.of(resultSetGenKeys, affectedRows);
        }).handle((tuple, sink) -> {
            try
            {
                JdbcResult result = null;

                if (this.returnGeneratedValues)
                {
                    // Generierte Auto_Increments liefern.
                    result = createResult(tuple.getT1(), tuple.getT2().length);
                }
                else
                {
                    // Nur die AffectedRows liefern.
                    result = createResultAffectedRows(tuple.getT1(), tuple.getT2());
                }

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
     * @see io.r2dbc.spi.Statement#returnGeneratedValues(java.lang.String[])
     */
    @Override
    public Statement returnGeneratedValues(final String...columns)
    {
        this.returnGeneratedValues = true;

        return this;
    }
}
