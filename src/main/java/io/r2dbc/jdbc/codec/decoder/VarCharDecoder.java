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
public class VarCharDecoder extends AbstractSqlDecoder<String>
{
    /**
     * Erstellt ein neues {@link VarCharDecoder} Object.
     */
    public VarCharDecoder()
    {
        super(JDBCType.VARCHAR.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.SqlDecoder#decode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public String decode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        String value = resultSet.getString(columnLabel);

        if (resultSet.wasNull())
        {
            return null;
        }

        return value;
    }
}
