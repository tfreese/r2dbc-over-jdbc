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
public class FloatDecoder extends AbstractSqlDecoder<Float>
{
    /**
     * Erstellt ein neues {@link FloatDecoder} Object.
     */
    public FloatDecoder()
    {
        super(JDBCType.FLOAT.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.SqlDecoder#decode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Float decode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        float value = resultSet.getFloat(columnLabel);

        if (resultSet.wasNull())
        {
            return null;
        }

        return value;
    }
}
