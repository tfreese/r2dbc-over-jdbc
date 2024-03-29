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
public class StringCodec extends AbstractCodec<String> {
    public StringCodec() {
        super(String.class, JDBCType.CHAR, JDBCType.VARCHAR);
    }

    @Override
    public String mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException {
        final String value = resultSet.getString(columnLabel);

        if (resultSet.wasNull()) {
            return null;
        }

        return value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <M> M mapTo(final Class<M> javaType, final String value) {
        if (value == null) {
            return null;
        }

        if (getJavaType().equals(javaType) || Object.class.equals(javaType)) {
            return (M) value;
        }
        else if (Blob.class.isAssignableFrom(javaType)) {
            final Blob blob = R2dbcUtils.stringToBlob(value);

            return (M) blob;
        }
        else if (Clob.class.isAssignableFrom(javaType)) {
            final Clob clob = R2dbcUtils.stringToClob(value);

            return (M) clob;
        }

        throw throwCanNotMapException(value);
    }

    @Override
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final String value) throws SQLException {
        preparedStatement.setString(parameterIndex, value);
    }
}
