/**
 * Created: 12.06.2019
 */

package io.r2dbc.jdbc;

import org.reactivestreams.Publisher;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public class JdbcStatement implements Statement
{
    /**
     * Erstellt ein neues {@link JdbcStatement} Object.
     */
    public JdbcStatement()
    {
        super();
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
     * @see io.r2dbc.spi.Statement#bind(java.lang.Object, java.lang.Object)
     */
    @Override
    public Statement bind(final Object identifier, final Object value)
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
     * @see io.r2dbc.spi.Statement#bindNull(java.lang.Object, java.lang.Class)
     */
    @Override
    public Statement bindNull(final Object identifier, final Class<?> type)
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
