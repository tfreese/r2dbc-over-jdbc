// Created: 27.03.2021
package io.r2dbc.jdbc.codecs;

import java.lang.reflect.ParameterizedType;
import java.util.Objects;

/**
 * Basic-Implementation for a {@link Codec}.
 *
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
     * Erstellt ein neues {@link AbstractCodec} Object.
     */
    @SuppressWarnings("unchecked")
    protected AbstractCodec()
    {
        super();

        this.javaType = (Class<T>) ((ParameterizedType) (getClass().getGenericSuperclass())).getActualTypeArguments()[0];
    }

    /**
     * Erstellt ein neues {@link AbstractCodec} Object.
     *
     * @param javaType Class
     */
    protected AbstractCodec(final Class<T> javaType)
    {
        super();

        this.javaType = Objects.requireNonNull(javaType, "javaType required");
    }

    /**
     * @return Class<T>
     */
    protected Class<T> getJavaType()
    {
        return this.javaType;
    }

    /**
     * @param object Object
     * @return {@link RuntimeException}
     */
    protected RuntimeException throwCanNotMapException(final Object object)
    {
        return new IllegalArgumentException(String.format("can not map %s into %s", object.getClass().getSimpleName(), getJavaType().getSimpleName()));
    }
}
