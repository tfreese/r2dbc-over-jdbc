/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.decoder;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Thomas Freese
 */
public class CharDecoder extends AbstractDecoder<Character>
{
    /**
     * Erstellt ein neues {@link CharDecoder} Object.
     */
    public CharDecoder()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.AbstractDecoder#checkWasNull(java.sql.ResultSet, java.lang.Object)
     */
    @Override
    protected Character checkWasNull(final ResultSet resultSet, final Character value) throws SQLException
    {
        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.AbstractDecoder#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Character doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        String value = resultSet.getString(columnLabel);

        if (resultSet.wasNull())
        {
            return null;
        }

        if ((value != null) && !value.isEmpty())
        {
            return value.charAt(0);
        }

        return null;
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.Decoder#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return JDBCType.CHAR.getVendorTypeNumber();
    }
}
