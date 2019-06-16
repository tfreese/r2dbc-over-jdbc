/**
 * Created: 12.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import io.r2dbc.spi.Statement;

/**
 * R2DBC Adapter for JDBC.<br>
 * Only for INSERT Statements.
 *
 * @author Thomas Freese
 */
public class JdbcPreparedStatementInsert extends AbstractJdbcStatement
{
    /**
    *
    */
    private boolean returnGeneratedValues = false;

    /**
     * Erstellt ein neues {@link JdbcPreparedStatementInsert} Object.
     *
     * @param preparedStatement {@link PreparedStatement}
     */
    public JdbcPreparedStatementInsert(final PreparedStatement preparedStatement)
    {
        super(preparedStatement);
    }

    /**
     * @see io.r2dbc.jdbc.AbstractJdbcStatement#createResult(java.sql.ResultSet, int[])
     */
    @Override
    protected JdbcResult createResult(final ResultSet resultSet, final int[] affectedRows) throws SQLException
    {
        if (this.returnGeneratedValues)
        {
            return super.createResult(resultSet, affectedRows);
        }

        return super.createResultAffectedRows(resultSet, affectedRows);
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
