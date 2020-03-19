/**
 * Created: 19.03.2020
 */

package io.r2dbc.jdbc.converter.sql;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Set;

/**
 * @author Thomas Freese
 */
public class DateSqlMapper extends AbstractSqlMapper<Date>
{
    /**
     * Erstellt ein neues {@link DateSqlMapper} Object.
     */
    public DateSqlMapper()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#mapFromSql(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public Date mapFromSql(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        java.sql.Date value = resultSet.getDate(columnLabel);

        if (resultSet.wasNull())
        {
            return null;
        }

        // java.sql.Date -> java.util.Date
        return new Date(value.getTime());
    }

    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#mapToSql(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    public void mapToSql(final PreparedStatement preparedStatement, final int parameterIndex, final Date value) throws SQLException
    {
        java.sql.Date date = new java.sql.Date(value.getTime());

        preparedStatement.setDate(parameterIndex, date);
    }

    /**
     * @see io.r2dbc.jdbc.converter.sql.SqlMapper#getSupportedJdbcTypes()
     */
    @Override
    public Set<JDBCType> getSupportedJdbcTypes()
    {
        return Set.of(JDBCType.DATE, JDBCType.TIME, JDBCType.TIMESTAMP);
    }
}
