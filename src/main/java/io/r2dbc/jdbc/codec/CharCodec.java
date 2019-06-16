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
public class CharCodec extends AbstractCodec<Character>
{
    /**
     * Erstellt ein neues {@link CharCodec} Object.
     */
    protected CharCodec()
    {
        super(Character.class, JDBCType.BIT.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Character doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        String value = resultSet.getString(columnLabel);

        if (value != null)
        {
            return value.charAt(0);
        }

        return null;
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#encodeNullSafe(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Character value) throws SQLException
    {
        preparedStatement.setString(parameterIndex, value.toString());
    }
}
