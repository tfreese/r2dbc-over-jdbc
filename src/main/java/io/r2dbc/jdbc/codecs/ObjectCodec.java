// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Thomas Freese
 */
public class ObjectCodec extends AbstractCodec<Object> {
    public ObjectCodec() {
        super(Object.class, JDBCType.OTHER);
    }

    @Override
    public Object mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException {
        final Object value = resultSet.getObject(columnLabel);

        if (resultSet.wasNull()) {
            return null;
        }

        return value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <M> M mapTo(final Class<M> javaType, final Object value) {
        if (value == null) {
            return null;
        }

        if (getJavaType().equals(javaType) || Object.class.equals(javaType)) {
            return (M) value;
        }
        else if (CharSequence.class.isAssignableFrom(javaType)) {
            final String s = value.toString();

            return (M) s;
        }

        throw throwCanNotMapException(value);
    }

    @Override
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Object value) throws SQLException {
        preparedStatement.setObject(parameterIndex, value);
    }
}
