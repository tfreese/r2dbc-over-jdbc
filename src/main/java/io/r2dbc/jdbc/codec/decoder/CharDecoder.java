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
public class CharDecoder extends AbstractSqlDecoder<Character>
{
    /**
     * Erstellt ein neues {@link CharDecoder} Object.
     */
    public CharDecoder()
    {
        super(JDBCType.CHAR.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.SqlDecoder#decode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Character decode(final ResultSet resultSet, final String columnLabel) throws SQLException
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
}
