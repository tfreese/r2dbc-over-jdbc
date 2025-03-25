// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Thomas Freese
 */
public class BooleanCodec extends AbstractCodec<Boolean> {
    public BooleanCodec() {
        super(Boolean.class, JDBCType.BIT, JDBCType.BOOLEAN);
    }

    @SuppressWarnings("java:S2447")
    @Override
    public Boolean mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException {
        final boolean value = resultSet.getBoolean(columnLabel);

        if (resultSet.wasNull()) {
            return null;
        }

        return value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <M> M mapTo(final Class<M> javaType, final Boolean value) {
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
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Boolean value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(parameterIndex, JDBCType.BOOLEAN.getVendorTypeNumber());
        }
        else {
            preparedStatement.setBoolean(parameterIndex, value);
        }
    }
}
