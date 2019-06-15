/**
 * Created: 12.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.PreparedStatement;
import java.util.Arrays;
import io.r2dbc.spi.Statement;

/**
 * R2DBC Adapter for JDBC.<br>
 * Only for DELETE Statements.
 *
 * @author Thomas Freese
 */
public class JdbcPreparedStatementDelete extends JdbcPreparedStatementInsertUpdate
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
     * @see io.r2dbc.jdbc.JdbcPreparedStatementInsertUpdate#checkAffectedRows(int[])
     */
    @Override
    protected int[] checkAffectedRows(final int[] affectedRows)
    {
        int[] rows = new int[affectedRows[0]];
        Arrays.fill(rows, 1);

        return rows;
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
