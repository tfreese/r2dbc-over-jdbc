/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec.encoder;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Fallback-Encoder.
 *
 * @author Thomas Freese
 */
public class ObjectEncoder extends AbstractEncoder<Object>
{
    /**
     * Fallback-Encoder.
     */
    public static final Encoder<Object> INSTANCE = new ObjectEncoder();

    /**
     * Erstellt ein neues {@link ObjectEncoder} Object.
     */
    public ObjectEncoder()
    {
        super(Object.class, JDBCType.OTHER.getVendorTypeNumber());
    }

    /**
     * @see AbstractEncoder#encodeNullSafe(PreparedStatement, int, Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Object value) throws SQLException
    {
        preparedStatement.setObject(parameterIndex, value);
    }
}
