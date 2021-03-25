// Created: 14.06.2019
package io.r2dbc.jdbc.converter.sql;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

import io.r2dbc.jdbc.util.R2dbcUtils;
import io.r2dbc.spi.Clob;
import reactor.core.publisher.Mono;

/**
 * @author Thomas Freese
 */
public class ClobSqlMapper extends AbstractSqlMapper<Clob>
{
    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#getSupportedJdbcTypes()
     */
    @Override
    public Set<JDBCType> getSupportedJdbcTypes()
    {
        return Set.of(JDBCType.CLOB);
    }

    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#mapFromSql(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Clob mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        java.sql.Clob value = resultSet.getClob(columnLabel);

        if (resultSet.wasNull())
        {
            return Clob.from(Mono.empty());
        }

        Clob clob = R2dbcUtils.sqlClobToClob(value);

        return clob;
    }

    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#mapToSql(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @SuppressWarnings("resource")
    @Override
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Clob value) throws SQLException
    {
        java.sql.Clob clob = preparedStatement.getConnection().createClob();

        String string = R2dbcUtils.clobToString(value);

        clob.setString(1, string);

        preparedStatement.setClob(parameterIndex, clob);
    }
}
