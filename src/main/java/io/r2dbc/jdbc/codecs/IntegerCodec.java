// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Thomas Freese
 */
public class IntegerCodec extends AbstractNumberCodec<Integer> {
    public IntegerCodec() {
        super(Integer.class, JDBCType.INTEGER);
    }

    @Override
    public Integer mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException {
        final int value = resultSet.getInt(columnLabel);

        if (resultSet.wasNull()) {
            return null;
        }

        return value;
    }

    @Override
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Integer value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(parameterIndex, JDBCType.INTEGER.getVendorTypeNumber());
        }
        else {
            preparedStatement.setInt(parameterIndex, value);
        }
    }
}
