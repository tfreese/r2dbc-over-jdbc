// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.r2dbc.jdbc.util.R2dbcUtils;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;

/**
 * @author Thomas Freese
 */
public class StringCodec extends AbstractCodec<String>
{
    /**
     * Erstellt ein neues {@link StringCodec} Object.
     */
    public StringCodec()
    {
        super(String.class);
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#canMapFromSql(java.sql.JDBCType)
     */
    @Override
    public boolean canMapFromSql(final JDBCType jdbcType)
    {
        return (JDBCType.CHAR == jdbcType) || (JDBCType.VARCHAR == jdbcType);
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#canMapTo(java.sql.JDBCType, java.lang.Class)
     */
    @Override
    public boolean canMapTo(final JDBCType jdbcType, final Class<?> type)
    {
        return true;
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

        return clazz.isInstance(CharSequence.class);
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#map(java.lang.Object)
     */
    @Override
    public String map(final Object value)
    {
        if (value instanceof Blob)
        {
            Blob blob = (Blob) value;

            String string = R2dbcUtils.blobToString(blob);

            return string;
        }
        else if (value instanceof Clob)
        {
            Clob clob = (Clob) value;

            String string = R2dbcUtils.clobToString(clob);

            return string;
        }

        if (value == null)
        {
            return null;
        }

        return value.toString();
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#mapFromSql(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public String mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        String value = resultSet.getString(columnLabel);

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
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final String value) throws SQLException
    {
        preparedStatement.setString(parameterIndex, value);
    }
}
