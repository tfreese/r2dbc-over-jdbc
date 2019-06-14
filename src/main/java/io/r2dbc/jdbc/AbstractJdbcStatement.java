/**
 * Created: 14.06.2019
 */

package io.r2dbc.jdbc;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.r2dbc.spi.Statement;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public abstract class AbstractJdbcStatement implements Statement
{
    /**
    *
    */
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
    *
    */
    private final java.sql.PreparedStatement preparedStatement;

    /**
     * Erstellt ein neues {@link AbstractJdbcStatement} Object.
     *
     * @param preparedStatement {@link java.sql.PreparedStatement}
     */
    public AbstractJdbcStatement(final java.sql.PreparedStatement preparedStatement)
    {
        super();

        this.preparedStatement = Objects.requireNonNull(preparedStatement, "statement must not be null");
    }

    /**
     * @see io.r2dbc.spi.Statement#bind(java.lang.Object, java.lang.Object)
     */
    @Override
    public Statement bind(final Object identifier, final Object value)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @see io.r2dbc.spi.Statement#bindNull(java.lang.Object, java.lang.Class)
     */
    @Override
    public Statement bindNull(final Object identifier, final Class<?> type)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @return {@link Logger}
     */
    protected Logger getLogger()
    {
        return this.logger;
    }

    /**
     * @return {@link java.sql.PreparedStatement}
     */
    protected java.sql.PreparedStatement getStatement()
    {
        return this.preparedStatement;
    }
}
