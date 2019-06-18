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
public class FloatDecoder extends AbstractDecoder<Float>
{
    /**
     * Erstellt ein neues {@link FloatDecoder} Object.
     */
    public FloatDecoder()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.Decoder#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return JDBCType.FLOAT.getVendorTypeNumber();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.AbstractDecoder#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Float doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        float value = resultSet.getFloat(columnLabel);

        return value;
    }
}
