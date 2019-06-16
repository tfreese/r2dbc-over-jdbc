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
public class VarCharCodec extends AbstractCodec<String>
{
    /**
     * Erstellt ein neues {@link VarCharCodec} Object.
     */
    public VarCharCodec()
    {
        super(String.class, JDBCType.VARCHAR.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#doDecode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    protected String doDecode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        String value = resultSet.getString(columnLabel);

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codec.AbstractCodec#encodeNullSafe(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    protected void encodeNullSafe(final PreparedStatement preparedStatement, final int parameterIndex, final String value) throws SQLException
    {
        preparedStatement.setString(parameterIndex, value);
    }
}
