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
public class NumericDecoder extends AbstractSqlDecoder<Double>
{
    /**
     * Erstellt ein neues {@link NumericDecoder} Object.
     */
    public NumericDecoder()
    {
        super(JDBCType.DOUBLE.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.decoder.SqlDecoder#decode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Double decode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        double value = resultSet.getDouble(columnLabel);

        if (resultSet.wasNull())
        {
            return null;
        }

        return value;
    }
}
