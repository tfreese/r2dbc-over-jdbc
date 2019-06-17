/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Thomas Freese
 */
public class DoubleCodec extends AbstractCodec<Double>
{
    /**
     * Erstellt ein neues {@link DoubleCodec} Object.
     */
    protected DoubleCodec()
    {
        super(Double.class, JDBCType.DOUBLE.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Double doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        Double value = resultSet.getDouble(columnLabel);

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#encodeNullSafe(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Double value) throws SQLException
    {
        preparedStatement.setDouble(parameterIndex, value);
    }
}
