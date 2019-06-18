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
public class BooleanEncoder extends AbstractEncoder<Boolean>
{
    /**
     * Erstellt ein neues {@link BooleanEncoder} Object.
     */
    public BooleanEncoder()
    {
        super(Boolean.class, JDBCType.BOOLEAN.getVendorTypeNumber());
    }

    /**
     * @see AbstractEncoder#encodeNullSafe(PreparedStatement, int, Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Boolean value) throws SQLException
    {
        preparedStatement.setBoolean(parameterIndex, value);
    }
}
