// Created: 14.06.2019
package io.r2dbc.jdbc.converter.transformer;

import io.r2dbc.spi.Row;

/**
 * Transforms an Object into another one.
 *
 * @see Row#get(int, Class)
 * @see Row#get(String, Class)
 * @author Thomas Freese
 * @param <T> Type
 */
public interface ObjectTransformer<T>
{
    /**
     * @return {@link Class}
     */
    public Class<T> getJavaType();

    /**
     * @param value Object
     * @return Object
     */
    public T transform(Object value);
}
