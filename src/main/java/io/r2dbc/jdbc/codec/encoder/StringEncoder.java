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
public class StringEncoder extends AbstractEncoder<String>
{
    /**
     * Erstellt ein neues {@link StringEncoder} Object.
     */
    public StringEncoder()
    {
        super(String.class, JDBCType.VARCHAR.getVendorTypeNumber());
    }

    /**
     * @see AbstractEncoder#encodeNullSafe(PreparedStatement, int, Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final String value) throws SQLException
    {
        preparedStatement.setString(parameterIndex, value);
    }
}