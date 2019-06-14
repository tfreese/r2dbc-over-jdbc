/**
 * Created: 12.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.PreparedStatement;
import org.reactivestreams.Publisher;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public class JdbcPreparedStatementUpdate extends AbstractJdbcStatement
{
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
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * @see io.r2dbc.spi.Statement#bind(int, java.lang.Object)
     */
    @Override
    public Statement bind(final int index, final Object value)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * @see io.r2dbc.spi.Statement#bindNull(int, java.lang.Class)
     */
    @Override
    public Statement bindNull(final int index, final Class<?> type)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * @see io.r2dbc.spi.Statement#execute()
     */
    @Override
    public Publisher<? extends Result> execute()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
