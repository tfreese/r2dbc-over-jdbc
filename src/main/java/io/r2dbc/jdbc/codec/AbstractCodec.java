/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;

/**
 * @author Thomas Freese
 * @param <T> Type
 */
public abstract class AbstractCodec<T> implements Codec<T>
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
     * Erstellt ein neues {@link AbstractCodec} Object.
     *
     * @param javaType {@link Class}
     * @param sqlType int, {@link Types}
     */
    public AbstractCodec(final Class<T> javaType, final int sqlType)
    {
        super();

        this.javaType = Objects.requireNonNull(javaType, "javaType must not be null");
        this.sqlType = sqlType;
    }

    /**
     * Erstellt ein neues {@link AbstractCodec} Object.
     *
     * @param javaType {@link Class}
     * @param sqlType {@link JDBCType}
     */
    public AbstractCodec(final Class<T> javaType, final JDBCType sqlType)
    {
        this(javaType, sqlType.getVendorTypeNumber());
    }

    /**
     * @see io.r2dbc.jdbc.codec.Codec#decode(java.sql.ResultSet, java.lang.String)
     */
    @Override
    public final T decode(final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        T value = doDecode(resultSet, columnLabel);

        if (resultSet.wasNull())
        {
            return null;
        }

        return value;
    }

    /**
     * @param resultSet {@link ResultSet}
     * @param columnLabel String
     * @return Object
     * @throws SQLException Falls was schief geht.
     */
    protected abstract T doDecode(ResultSet resultSet, String columnLabel) throws SQLException;

    /**
     * @see io.r2dbc.jdbc.codec.Codec#encode(java.sql.PreparedStatement, int, java.lang.Object)
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
     * @see io.r2dbc.jdbc.codec.Codec#getJavaType()
     */
    @Override
    public Class<T> getJavaType()
    {
        return this.javaType;
    }

    /**
     * @see io.r2dbc.jdbc.codec.Codec#getSqlType()
     */
    @Override
    public int getSqlType()
    {
        return this.sqlType;
    }
}
