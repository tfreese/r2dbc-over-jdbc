// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.sql.JDBCType;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Basic-Implementation for a {@link Codec}.
 *
 * @param <T> Type
 *
 * @author Thomas Freese
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
    private final Set<JDBCType> supportedJdbcTypes;

    // /**
    // * Erstellt ein neues {@link AbstractCodec} Object.
    // */
    // @SuppressWarnings("unchecked")
    // protected AbstractCodec()
    // {
    // super();
    //
    // this.javaType = (Class<T>) ((ParameterizedType) (getClass().getGenericSuperclass())).getActualTypeArguments()[0];
    // }

    /**
     * Erstellt ein neues {@link AbstractCodec} Object.
     *
     * @param javaType Class
     * @param supportedJdbcTypes {@link JDBCType}
     */
    protected AbstractCodec(final Class<T> javaType, final JDBCType... supportedJdbcTypes)
    {
        super();

        this.javaType = Objects.requireNonNull(javaType, "javaType required");

        //        if (supportedJdbcTypes.length == 0)
        //        {
        //            throw new IllegalArgumentException("at leat one JDBCType required");
        //        }

        this.supportedJdbcTypes = Stream.of(supportedJdbcTypes).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#getJavaType()
     */
    @Override
    public Class<T> getJavaType()
    {
        return this.javaType;
    }

    /**
     * @see io.r2dbc.jdbc.codecs.Codec#supportedJdbcTypes()
     */
    @Override
    public Set<JDBCType> supportedJdbcTypes()
    {
        return this.supportedJdbcTypes;
    }

    /**
     * @param object Object
     *
     * @return {@link RuntimeException}
     */
    protected RuntimeException throwCanNotMapException(final Object object)
    {
        return new IllegalArgumentException(String.format("can not map %s into %s", object.getClass().getSimpleName(), getJavaType().getSimpleName()));
    }
}
