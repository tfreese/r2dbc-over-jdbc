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
public class BigIntCodec extends AbstractCodec<Long>
{
    /**
     * Erstellt ein neues {@link BigIntCodec} Object.
     */
    public BigIntCodec()
    {
        super(Long.class, JDBCType.BIGINT.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected Long doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        long value = resultSet.getLong(columnLabel);

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#encodeNullSafe(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final Long value) throws SQLException
    {
        preparedStatement.setLong(parameterIndex, value);
    }
}
