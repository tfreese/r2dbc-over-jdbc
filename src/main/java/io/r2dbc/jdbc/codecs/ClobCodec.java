// Created: 28.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.r2dbc.jdbc.util.R2dbcUtils;
import io.r2dbc.spi.Clob;
import reactor.core.publisher.Mono;

/**
 * @author Thomas Freese
 */
public class ClobCodec extends AbstractCodec<Clob> {
    public static final Clob NULL_CLOB = Clob.from(Mono.empty());

    public ClobCodec() {
        super(Clob.class, JDBCType.CLOB);
    }

    @Override
    public Clob mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException {
        java.sql.Clob value = resultSet.getClob(columnLabel);

        if (resultSet.wasNull()) {
            return NULL_CLOB;
        }

        return R2dbcUtils.sqlClobToClob(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <M> M mapTo(final Class<M> javaType, final Clob value) {
        if (value == null) {
            return null;
        }

        if (getJavaType().equals(javaType) || Object.class.equals(javaType)) {
            // if (NULL_CLOB.equals(value))
            // {
            // return null;
            // }

            // return javaType.cast(R2dbcUtils.clobToString(value));

            return (M) value;
        }
        else if (CharSequence.class.isAssignableFrom(javaType)) {
            return (M) R2dbcUtils.clobToString(value);
        }

        throw throwCanNotMapException(value);
    }

    @Override
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Clob value) throws SQLException {
        java.sql.Clob clob = preparedStatement.getConnection().createClob();

        String string = R2dbcUtils.clobToString(value);

        clob.setString(1, string);

        preparedStatement.setClob(parameterIndex, clob);
    }
}
