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
public class BooleanDecoder extends AbstractSqlDecoder<Boolean>
{
    /**
     * Erstellt ein neues {@link BooleanDecoder} Object.
     */
    public BooleanDecoder()
    {
        super(JDBCType.BOOLEAN.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.SqlDecoder#decode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Boolean decode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        boolean value = resultSet.getBoolean(columnLabel);

        if (resultSet.wasNull())
        {
            return null;
        }

        return value;
    }
}
