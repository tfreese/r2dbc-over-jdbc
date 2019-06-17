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
public class SmallIntCodec extends AbstractCodec<Short>
{
    /**
     * Erstellt ein neues {@link SmallIntCodec} Object.
     */
    protected SmallIntCodec()
    {
        super(Short.class, JDBCType.SMALLINT.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Short doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        Short value = resultSet.getShort(columnLabel);

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#encodeNullSafe(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Short value) throws SQLException
    {
        preparedStatement.setShort(parameterIndex, value);
    }
}
