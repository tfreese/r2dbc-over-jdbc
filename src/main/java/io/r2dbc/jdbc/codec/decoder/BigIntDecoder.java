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
public class BigIntDecoder extends AbstractDecoder<Long>
{
    /**
     * Erstellt ein neues {@link BigIntDecoder} Object.
     */
    public BigIntDecoder()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.AbstractDecoder#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Long doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        long value = resultSet.getLong(columnLabel);

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.Decoder#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return JDBCType.BIGINT.getVendorTypeNumber();
    }
}
