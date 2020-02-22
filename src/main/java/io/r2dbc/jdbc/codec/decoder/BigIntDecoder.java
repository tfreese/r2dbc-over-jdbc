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
public class BigIntDecoder extends AbstractSqlDecoder<Long>
{
    /**
     * Erstellt ein neues {@link BigIntDecoder} Object.
     */
    public BigIntDecoder()
    {
        super(JDBCType.BIGINT.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.SqlDecoder#decode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Long decode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        long value = resultSet.getLong(columnLabel);

        if (resultSet.wasNull())
        {
            return null;
        }

        return value;
    }
}
