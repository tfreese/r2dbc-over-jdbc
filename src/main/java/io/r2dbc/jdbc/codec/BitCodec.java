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
public class BitCodec extends AbstractCodec<Boolean>
{
    /**
     * Erstellt ein neues {@link BitCodec} Object.
     */
    protected BitCodec()
    {
        super(Boolean.class, JDBCType.BIT.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Boolean doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        boolean value = resultSet.getBoolean(columnLabel);

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#encodeNullSafe(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Boolean value) throws SQLException
    {
        preparedStatement.setBoolean(parameterIndex, value);
    }
}
