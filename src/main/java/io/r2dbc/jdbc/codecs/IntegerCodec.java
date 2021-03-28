// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Thomas Freese
 */
public class IntegerCodec extends AbstractNumberCodec<Integer>
{
    /**
     * Erstellt ein neues {@link IntegerCodec} Object.
     */
    public IntegerCodec()
    {
        super(Integer.class, JDBCType.INTEGER);
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#mapFromSql(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Integer mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        int value = resultSet.getInt(columnLabel);

        if (resultSet.wasNull())
        {
            return null;
        }

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#mapToSql(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Integer value) throws SQLException
    {
        preparedStatement.setInt(parameterIndex, value);
    }
}
