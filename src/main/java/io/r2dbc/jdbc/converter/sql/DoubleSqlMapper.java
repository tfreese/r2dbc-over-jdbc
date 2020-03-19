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
public class DoubleSqlMapper extends AbstractSqlMapper<Double>
{
    /**
     * Erstellt ein neues {@link DoubleSqlMapper} Object.
     */
    public DoubleSqlMapper()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#mapFromSql(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Double mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        double value = resultSet.getDouble(columnLabel);

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
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Double value) throws SQLException
    {
        preparedStatement.setDouble(parameterIndex, value);
    }

    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#getSupportedJdbcTypes()
     */
    @Override
    public Set<JDBCType> getSupportedJdbcTypes()
    {
        return Set.of(JDBCType.DOUBLE);
    }
}
