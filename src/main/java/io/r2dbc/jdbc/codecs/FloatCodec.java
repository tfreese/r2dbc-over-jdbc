// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Thomas Freese
 */
public class FloatCodec extends AbstractNumberCodec<Float> {
    public FloatCodec() {
        super(Float.class, JDBCType.FLOAT);
    }

    @Override
    public Float mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException {
        final float value = resultSet.getFloat(columnLabel);

        if (resultSet.wasNull()) {
            return null;
        }

        return value;
    }

    @Override
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Float value) throws SQLException {
        if (value == null) {
            preparedStatement.setNull(parameterIndex, JDBCType.FLOAT.getVendorTypeNumber());
        }
        else {
            preparedStatement.setFloat(parameterIndex, value);
        }
    }
}
