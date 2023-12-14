// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Thomas Freese
 */
public class LongCodec extends AbstractNumberCodec<Long> {
    public LongCodec() {
        super(Long.class, JDBCType.BIGINT, JDBCType.DECIMAL);
    }

    @Override
    public Long mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException {
        final long value = resultSet.getLong(columnLabel);

        if (resultSet.wasNull()) {
            return null;
        }

        return value;
    }

    @Override
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Long value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(parameterIndex, JDBCType.DECIMAL.getVendorTypeNumber());
        }
        else {
            preparedStatement.setLong(parameterIndex, value);
        }
    }
}
