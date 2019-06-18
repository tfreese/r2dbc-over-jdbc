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
public class DoubleDecoder extends AbstractDecoder<Double>
{
    /**
     * Erstellt ein neues {@link DoubleDecoder} Object.
     */
    public DoubleDecoder()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.Decoder#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return JDBCType.DOUBLE.getVendorTypeNumber();
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.AbstractDecoder#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Double doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        double value = resultSet.getDouble(columnLabel);

        return value;
    }
}
