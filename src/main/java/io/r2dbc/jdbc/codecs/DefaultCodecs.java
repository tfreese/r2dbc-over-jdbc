// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Default implementation of {@link Codecs}.
 *
 * @author Thomas Freese
 */
public class DefaultCodecs implements Codecs
{
    /**
     *
     */
    private final List<Codec<?>> codecs = new ArrayList<>();

    /**
     * Erstellt ein neues {@link DefaultCodecs} Object.
     */
    public DefaultCodecs()
    {
        super();

        add(new IntegerCodec());
        add(new StringCodec());
    }

    /**
     * @param codec {@link Codec}
     */
    public void add(final Codec<?> codec)
    {
        if (this.codecs.contains(codec))
        {
            return;
        }

        this.codecs.add(codec);
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codecs#mapFromSql(java.sql.JDBCType, java.sql.ResultSet, java.lang.String)
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T mapFromSql(final JDBCType jdbcType, final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        Objects.requireNonNull(jdbcType, "jdbcType must not be null");
        Objects.requireNonNull(resultSet, "resultSet must not be null");
        Objects.requireNonNull(columnLabel, "columnLabel must not be null");

        for (Codec<?> codec : this.codecs)
        {
            if (codec.canMapFromSql(jdbcType))
            {
                return ((Codec<T>) codec).mapFromSql(resultSet, columnLabel);
            }
        }

        throw new IllegalArgumentException(String.format("Can not decode value from sql of type %s", jdbcType.getName()));
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codecs#mapTo(java.sql.JDBCType, java.lang.Class, java.lang.Object)
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T mapTo(final JDBCType jdbcType, final Class<? extends T> type, final Object value)
    {
        Objects.requireNonNull(jdbcType, "jdbcType must not be null");
        Objects.requireNonNull(type, "type must not be null");

        for (Codec<?> codec : this.codecs)
        {
            if (codec.canMapTo(jdbcType, type))
            {
                return ((Codec<T>) codec).map(value);
            }
        }

        if (value == null)
        {
            throw new IllegalArgumentException(String.format("Cannot map value to %s", type.getSimpleName()));
        }

        throw new IllegalArgumentException(String.format("Can not map %s to %s", value.getClass().getSimpleName(), type.getSimpleName()));
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codecs#mapToSql(java.sql.JDBCType, java.sql.PreparedStatement, int, java.lang.Object)
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> void mapToSql(final JDBCType jdbcType, final PreparedStatement preparedStatement, final int parameterIndex, final T value) throws SQLException
    {
        Objects.requireNonNull(jdbcType, "jdbcType must not be null");
        Objects.requireNonNull(preparedStatement, "preparedStatement must not be null");
        Objects.checkIndex(parameterIndex, Integer.MAX_VALUE);

        for (Codec<?> codec : this.codecs)
        {
            if (codec.canMapToSql(jdbcType, value))
            {
                ((Codec<T>) codec).mapToSql(preparedStatement, parameterIndex, value);
            }
        }

        throw new IllegalArgumentException(String.format("Can not encode parameter to sql of type %s", value.getClass().getName()));
    }
}
