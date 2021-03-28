// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Thomas Freese
 */
public class BooleanCodec extends AbstractCodec<Boolean>
{
    /**
     * Erstellt ein neues {@link BooleanCodec} Object.
     */
    public BooleanCodec()
    {
        super(Boolean.class, JDBCType.BIT, JDBCType.BOOLEAN);
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#mapFromSql(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Boolean mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        boolean value = resultSet.getBoolean(columnLabel);

        if (resultSet.wasNull())
        {
            return null;
        }

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#mapTo(java.lang.Class, java.lang.Object)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <M> M mapTo(final Class<M> javaType, final Boolean value)
    {
        if (value == null)
        {
            return null;
        }

        if (getJavaType().equals(javaType) || Object.class.equals(javaType))
        {
            return (M) value;
        }
        else if (CharSequence.class.isAssignableFrom(javaType))
        {
            String s = value.toString();

            return (M) s;
        }

        throw throwCanNotMapException(value);
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#mapToSql(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Boolean value) throws SQLException
    {
        preparedStatement.setBoolean(parameterIndex, value);
    }
}
