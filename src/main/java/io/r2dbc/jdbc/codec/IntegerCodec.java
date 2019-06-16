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
public class IntegerCodec extends AbstractCodec<Integer>
{
    /**
     * Erstellt ein neues {@link IntegerCodec} Object.
     */
    public IntegerCodec()
    {
        super(Integer.class, JDBCType.INTEGER.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Integer doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        int value = resultSet.getInt(columnLabel);

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#encodeNullSafe(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Integer value) throws SQLException
    {
        preparedStatement.setInt(parameterIndex, value);
    }
}
