/**
 * Created: 12.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * R2DBC Adapter for JDBC.<br>
 * Only for DELETE Statements.
 *
 * @author Thomas Freese
 */
public class JdbcPreparedStatementDelete extends AbstractJdbcStatement
{
    /**
     * Erstellt ein neues {@link JdbcPreparedStatementDelete} Object.
     *
     * @param preparedStatement {@link PreparedStatement}
     */
    public JdbcPreparedStatementDelete(final PreparedStatement preparedStatement)
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
