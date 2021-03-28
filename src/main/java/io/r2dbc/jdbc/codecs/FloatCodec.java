// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Thomas Freese
 */
public class FloatCodec extends AbstractNumberCodec<Float>
{
    /**
     * Erstellt ein neues {@link FloatCodec} Object.
     */
    public FloatCodec()
    {
        super(Float.class, JDBCType.FLOAT);
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#mapFromSql(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Float mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        float value = resultSet.getFloat(columnLabel);

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
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Float value) throws SQLException
    {
        preparedStatement.setFloat(parameterIndex, value);
    }
}
