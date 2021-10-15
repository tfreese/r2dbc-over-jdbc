// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
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
    private final Map<Class<?>, Codec<?>> codecsForJavaType = new HashMap<>();
    /**
    *
    */
    private final Map<JDBCType, Codec<?>> codecsForJDBCType = new HashMap<>();

    /**
     * Erstellt ein neues {@link DefaultCodecs} Object.
     */
    public DefaultCodecs()
    {
        super();

        loadCodecs();
    }

    /**
     * @param codec {@link Codec}
     */
    public void add(final Codec<?> codec)
    {
        for (JDBCType jdbcType : codec.supportedJdbcTypes())
        {
            if (this.codecsForJDBCType.containsKey(jdbcType))
            {
                throw new IllegalArgumentException(String.format("JDBCType '%s' already supported by %s", jdbcType.getName(),
                        this.codecsForJDBCType.get(jdbcType).getClass().getSimpleName()));
            }

            this.codecsForJDBCType.put(jdbcType, codec);
        }

        if (this.codecsForJavaType.containsKey(codec.getJavaType()))
        {
            throw new IllegalArgumentException(String.format("JavaType '%s' already supported by %s", codec.getJavaType().getSimpleName(),
                    this.codecsForJavaType.get(codec.getJavaType()).getClass().getSimpleName()));
        }

        this.codecsForJavaType.put(codec.getJavaType(), codec);
    }

    /**
     * Returns the {@link Codec} for the {@link JDBCType}
     *
     * @param javaType Class
     *
     * @return {@link Codec}
     */
    @SuppressWarnings("unchecked")
    protected <T> Codec<T> get(final Class<T> javaType)
    {
        Objects.requireNonNull(javaType, "javaType must not be null");

        Codec<?> codec = this.codecsForJavaType.get(javaType);

        if (codec == null)
        {
            codec = this.codecsForJavaType.get(javaType.getEnclosingClass());
        }

        if (codec == null)
        {
            throw new IllegalArgumentException(String.format("No Codec found for JavaType %s", javaType.getSimpleName()));
        }

        return (Codec<T>) codec;
    }

    /**
     * Returns the {@link Codec} for the {@link JDBCType}
     *
     * @param jdbcType {@link JDBCType}
     *
     * @return {@link Codec}
     */
    @SuppressWarnings("unchecked")
    protected <T> Codec<T> get(final JDBCType jdbcType)
    {
        Objects.requireNonNull(jdbcType, "jdbcType must not be null");

        Codec<?> codec = this.codecsForJDBCType.get(jdbcType);

        if (codec == null)
        {
            throw new IllegalArgumentException(String.format("No Codec found for type %s", jdbcType.getName()));
        }

        return (Codec<T>) codec;
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codecs#getJavaType(java.sql.JDBCType)
     */
    @Override
    public Class<?> getJavaType(final JDBCType jdbcType)
    {
        return get(jdbcType).getJavaType();
    }

    /**
     *
     */
    protected void loadCodecs()
    {
        add(new BlobCodec());
        add(new BooleanCodec());
        add(new ClobCodec());
        add(new DateCodec());
        add(new DoubleCodec());
        add(new FloatCodec());
        add(new IntegerCodec());
        add(new LongCodec());
        add(new ObjectCodec());
        add(new StringCodec());
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codecs#mapFromSql(java.sql.JDBCType, java.sql.ResultSet, java.lang.String)
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T mapFromSql(final JDBCType jdbcType, final ResultSet resultSet, final String columnLabel) throws SQLException
    {
        Objects.requireNonNull(resultSet, "resultSet must not be null");
        Objects.requireNonNull(columnLabel, "columnLabel must not be null");

        Codec<?> codec = get(jdbcType);

        return ((Codec<T>) codec).mapFromSql(resultSet, columnLabel);
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codecs#mapTo(java.sql.JDBCType, java.lang.Class, java.lang.Object)
     */
    @Override
    @SuppressWarnings(
    {
            "unchecked", "rawtypes"
    })
    public <T> T mapTo(final JDBCType jdbcType, final Class<? extends T> javaType, final Object value)
    {
        Objects.requireNonNull(javaType, "javaType must not be null");

        Codec codec = get(jdbcType);

        return (T) codec.mapTo(javaType, value);
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codecs#mapToSql(java.lang.Class, java.sql.PreparedStatement, int, java.lang.Object)
     */
    @SuppressWarnings(
    {
            "rawtypes", "unchecked"
    })
    @Override
    public void mapToSql(final Class<?> javaType, final PreparedStatement preparedStatement, final int parameterIndex, final Object value) throws SQLException
    {
        Objects.requireNonNull(javaType, "javaType must not be null");
        Objects.requireNonNull(preparedStatement, "preparedStatement must not be null");
        Objects.checkIndex(parameterIndex, Integer.MAX_VALUE);

        Codec codec = get(javaType);

        codec.mapToSql(preparedStatement, parameterIndex, value);
    }
}
