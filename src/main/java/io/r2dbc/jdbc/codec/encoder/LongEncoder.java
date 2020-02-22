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
public class LongEncoder extends AbstractSqlEncoder<Long>
{
    /**
     * Erstellt ein neues {@link LongEncoder} Object.
     */
    public LongEncoder()
    {
        super(JDBCType.DECIMAL.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.encoder.AbstractSqlEncoder#encodeNullSafe(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Long value) throws SQLException
    {
        preparedStatement.setLong(parameterIndex, value);
    }
}
