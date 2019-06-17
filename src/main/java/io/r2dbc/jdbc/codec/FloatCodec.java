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
public class FloatCodec extends AbstractCodec<Float>
{
    /**
     * Erstellt ein neues {@link FloatCodec} Object.
     */
    protected FloatCodec()
    {
        super(Float.class, JDBCType.FLOAT.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Float doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        Float value = resultSet.getFloat(columnLabel);

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#encodeNullSafe(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Float value) throws SQLException
    {
        preparedStatement.setFloat(parameterIndex, value);
    }
}
