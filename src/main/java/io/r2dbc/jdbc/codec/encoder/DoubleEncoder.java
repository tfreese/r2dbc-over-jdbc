/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.encoder;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Thomas Freese
 */
public class DoubleEncoder extends AbstractSqlEncoder<Double>
{
    /**
     * Erstellt ein neues {@link DoubleEncoder} Object.
     */
    public DoubleEncoder()
    {
        super(JDBCType.DOUBLE.getVendorTypeNumber());
    }

    /**
     * @see AbstractSqlEncoder#encodeNullSafe(PreparedStatement, int, Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Double value) throws SQLException
    {
        preparedStatement.setDouble(parameterIndex, value);
    }
}
