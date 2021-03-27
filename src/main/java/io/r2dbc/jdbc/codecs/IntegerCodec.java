// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

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
        super(Integer.class);
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#canMapFromSql(java.sql.JDBCType)
     */
    @Override
    public boolean canMapFromSql(final JDBCType jdbcType)
    {
        return JDBCType.INTEGER == jdbcType;
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#canMapTo(java.sql.JDBCType, java.lang.Class)
     */
    @Override
    public boolean canMapTo(final JDBCType jdbcType, final Class<?> type)
    {
        return type.isInstance(Number.class) || type.isInstance(String.class);
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#canMapToSql(java.sql.JDBCType, java.lang.Object)
     */
    @Override
    public boolean canMapToSql(final JDBCType jdbcType, final Object value)
    {
        if (value == null)
        {
            return true;
        }

        Class<?> clazz = value.getClass();

        return clazz.isInstance(getJavaType());
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#map(java.lang.Object)
     */
    @Override
    public Integer map(final Object value)
    {
        if (value instanceof Number)
        {
            return ((Number) value).intValue();
        }
        else if (value instanceof String)
        {
            return Integer.valueOf((String) value);
        }

        if (value == null)
        {
            return null;
        }

        throw throwCanNotMapException(value);
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
