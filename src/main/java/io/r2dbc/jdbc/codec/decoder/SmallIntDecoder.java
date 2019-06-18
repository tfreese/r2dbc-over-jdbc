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
public class SmallIntDecoder extends AbstractDecoder<Short>
{
    /**
     * Erstellt ein neues {@link SmallIntDecoder} Object.
     */
    public SmallIntDecoder()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.Decoder#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return JDBCType.SMALLINT.getVendorTypeNumber();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.AbstractDecoder#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Short doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        short value = resultSet.getShort(columnLabel);

        return value;
    }
}
