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
public class IntegerEncoder extends AbstractEncoder<Integer>
{
    /**
     * Erstellt ein neues {@link IntegerEncoder} Object.
     */
    public IntegerEncoder()
    {
        super(Integer.class, JDBCType.INTEGER.getVendorTypeNumber());
    }

    /**
     * @see AbstractEncoder#encodeNullSafe(PreparedStatement, int, Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Integer value) throws SQLException
    {
        preparedStatement.setInt(parameterIndex, value);
    }
}