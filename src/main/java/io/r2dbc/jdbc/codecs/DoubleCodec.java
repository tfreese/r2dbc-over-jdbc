// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Thomas Freese
 */
public class DoubleCodec extends AbstractNumberCodec<Double> {
    public DoubleCodec() {
        super(Double.class, JDBCType.DOUBLE);
    }

    @Override
    public Double mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException {
        final double value = resultSet.getDouble(columnLabel);

        if (resultSet.wasNull()) {
            return null;
        }

        return value;
    }

    @Override
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Double value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(parameterIndex, JDBCType.DOUBLE.getVendorTypeNumber());
        }
        else {
            preparedStatement.setDouble(parameterIndex, value);
        }
    }
}
