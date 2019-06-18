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
public class BooleanDecoder extends AbstractDecoder<Boolean>
{
    /**
     * Erstellt ein neues {@link BooleanDecoder} Object.
     */
    public BooleanDecoder()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.AbstractDecoder#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Boolean doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        boolean value = resultSet.getBoolean(columnLabel);

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.Decoder#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return JDBCType.BOOLEAN.getVendorTypeNumber();
    }
}
