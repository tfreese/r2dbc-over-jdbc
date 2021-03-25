// Created: 14.06.2019
package io.r2dbc.jdbc.converter.sql;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author Thomas Freese
 */
public class ObjectSqlMapper extends AbstractSqlMapper<Object>
{
    /**
     * Fallback-Converter.
     */
    public static final SqlMapper<Object> INSTANCE = new ObjectSqlMapper();

    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#getSupportedJdbcTypes()
     */
    @Override
    public Set<JDBCType> getSupportedJdbcTypes()
    {
        return Set.of(JDBCType.OTHER);
    }

    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#mapFromSql(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Object mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        Object value = resultSet.getObject(columnLabel);

        if (resultSet.wasNull())
        {
            return null;
        }

        return value;
    }

    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#mapToSql(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Object value) throws SQLException
    {
        preparedStatement.setObject(parameterIndex, value);
    }
}
