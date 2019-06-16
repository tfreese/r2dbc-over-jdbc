/**
 * Created: 12.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * R2DBC Adapter for JDBC.<br>
 * Only for UPDATE Statements.
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
     * @see io.r2dbc.jdbc.AbstractJdbcStatement#createResult(java.sql.ResultSet, int[])
     */
    @Override
    protected JdbcResult createResult(final ResultSet resultSet, final int[] affectedRows) throws SQLException
    {
        return super.createResultAffectedRows(resultSet, affectedRows);
    }
}
