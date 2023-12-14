package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Parameters;

/**
 * @author Thomas Freese
 */
public class ParameterCodec extends AbstractCodec<Parameter> {
    private final Codecs codecs;

    public ParameterCodec(final Codecs codecs) {
        super(Parameter.class);

        this.codecs = codecs;
    }

    @Override
    public Parameter mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException {
        Object value = resultSet.getObject(columnLabel);

        if (value == null) {
            return null;
        }

        final JDBCType jdbcType = codecs.getJdbcType(value.getClass());
        value = codecs.mapFromSql(jdbcType, resultSet, columnLabel);

        return Parameters.inOut(value);
    }

    @Override
    public <M> M mapTo(final Class<M> javaType, final Parameter value) {
        final Object mappedObject = codecs.mapTo(codecs.getJdbcType(value.getClass()), value.getType().getJavaType(), value);

        return javaType.cast(mappedObject);
    }

    @Override
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Parameter value) throws SQLException {
        codecs.mapToSql(value.getType().getJavaType(), preparedStatement, parameterIndex, value.getValue());
    }
}
