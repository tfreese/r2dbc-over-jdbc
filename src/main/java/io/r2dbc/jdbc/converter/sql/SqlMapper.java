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
 * Mapping for a SQL-Type from the {@link ResultSet} to an Java-Object and vice versa.
 *
 * @author Thomas Freese
 * @param <T> Type
 */
public interface SqlMapper<T>
{
    /**
     * @param resultSet {@link ResultSet}
     * @param columnLabel String
     * @return Object
     * @throws SQLException Falls was schief geht.
     */
    public T mapFromSql(ResultSet resultSet, String columnLabel) throws SQLException;

    /**
     * @param preparedStatement {@link PreparedStatement}
     * @param parameterIndex int, ONE-Based
     * @param value Object
     * @throws SQLException Falls was schief geht.
     */
    public void mapToSql(PreparedStatement preparedStatement, int parameterIndex, T value) throws SQLException;

    /**
     * @return Object
     */
    public Class<T> getJavaType();

    /**
     * @return {@link Set}
     */
    public Set<JDBCType> getSupportedJdbcTypes();
}
