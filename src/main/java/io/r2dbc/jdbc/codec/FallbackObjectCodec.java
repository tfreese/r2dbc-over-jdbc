/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Thomas Freese
 */
public class FallbackObjectCodec extends AbstractCodec<Object>
{
    /**
     * Erstellt ein neues {@link FallbackObjectCodec} Object.
     */
    public FallbackObjectCodec()
    {
        super(Object.class, JDBCType.OTHER);
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Object doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        Object value = resultSet.getObject(columnLabel);

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#encodeNullSafe(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Object value) throws SQLException
    {
        preparedStatement.setObject(parameterIndex, value);
    }
}
