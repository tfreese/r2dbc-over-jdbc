/**
 * Created: 12.06.2019
 */

package io.r2dbc.jdbc;

import org.reactivestreams.Publisher;
import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Result;

/**
 * R2DBC Adapter for JDBC.
 * 
 * @author Thomas Freese
 */
public class JdbcBatch implements Batch
{
    /**
     * Erstellt ein neues {@link JdbcBatch} Object.
     */
    public JdbcBatch()
    {
        super();
    }

    /**
     * @see io.r2dbc.spi.Batch#add(java.lang.String)
     */
    @Override
    public Batch add(final String sql)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * @see io.r2dbc.spi.Batch#execute()
     */
    @Override
    public Publisher<? extends Result> execute()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
