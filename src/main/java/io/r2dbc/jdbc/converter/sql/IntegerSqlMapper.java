/**
 * Created: 19.03.2020
 */

package io.r2dbc.jdbc.converter.sql;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

/**
 * @author Thomas Freese
 */
public class IntegerSqlMapper extends AbstractSqlMapper<Integer>
{
    /**
     * Erstellt ein neues {@link IntegerSqlMapper} Object.
     */
    public IntegerSqlMapper()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#mapFromSql(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Integer mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        int value = resultSet.getInt(columnLabel);

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
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Integer value) throws SQLException
    {
        preparedStatement.setInt(parameterIndex, value);
    }

    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#getSupportedJdbcTypes()
     */
    @Override
    public Set<JDBCType> getSupportedJdbcTypes()
    {
        return Set.of(JDBCType.INTEGER);
    }
}
