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

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#mapFromSql(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Long mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException {
        long value = resultSet.getLong(columnLabel);

        if (resultSet.wasNull()) {
            return null;
        }

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#mapToSql(java.sql.PreparedStatement, int, java.lang.Object)
     */
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
