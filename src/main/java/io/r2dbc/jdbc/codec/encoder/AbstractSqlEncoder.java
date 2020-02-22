/**
 * Created: 18.06.2019
 */

package io.r2dbc.jdbc.codec.encoder;

import java.lang.reflect.ParameterizedType;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Encodes a Java-Object for a {@link PreparedStatement}.
 *
 * @param <T> Type
 * @author Thomas Freese
 */
public abstract class AbstractSqlEncoder<T> implements SqlEncoder<T>
{
    /**
     *
     */
    private final Class<T> javaType;

    /**
     *
     */
    private final int sqlType;

    /**
     * Erstellt ein neues {@link AbstractSqlEncoder} Object.
     *
     * @param sqlType int
     */
    @SuppressWarnings("unchecked")
    public AbstractSqlEncoder(final int sqlType)
    {
        super();

        this.sqlType = sqlType;

        // this.javaType = Objects.requireNonNull(javaType, "javaType must not be null");
        this.javaType = (Class<T>) ((ParameterizedType) (getClass().getGenericSuperclass())).getActualTypeArguments()[0];
    }

    /**
     * @see io.r2dbc.jdbc.codec.encoder.SqlEncoder#encode(java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    public final void encode(final PreparedStatement preparedStatement, final int parameterIndex, final T value) throws SQLException
    {
        if (value == null)
        {
            preparedStatement.setNull(parameterIndex, getSqlType());
        }
        else
        {
            encodeNullSafe(preparedStatement, parameterIndex, value);
        }
    }

    /**
     * @param preparedStatement {@link PreparedStatement}
     * @param parameterIndex int
     * @param value Object, not null
     * @throws SQLException Falls was schief geht.
     */
    protected abstract void encodeNullSafe(PreparedStatement preparedStatement, int parameterIndex, T value) throws SQLException;

    /**
     * @see io.r2dbc.jdbc.codec.encoder.SqlEncoder#getJavaType()
     */
    @Override
    public Class<T> getJavaType()
    {
        return this.javaType;
    }

    /**
     * @see Types
     * @see JDBCType
     * @return int
     */
    public int getSqlType()
    {
        return this.sqlType;
    }
}
