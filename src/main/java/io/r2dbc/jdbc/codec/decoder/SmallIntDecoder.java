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
public class SmallIntDecoder extends AbstractSqlDecoder<Short>
{
    /**
     * Erstellt ein neues {@link SmallIntDecoder} Object.
     */
    public SmallIntDecoder()
    {
        super(JDBCType.SMALLINT.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.SqlDecoder#decode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Short decode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        short value = resultSet.getShort(columnLabel);

        if (resultSet.wasNull())
        {
            return null;
        }

        return value;
    }
}
