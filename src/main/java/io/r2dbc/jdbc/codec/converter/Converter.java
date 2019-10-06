/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.codec.converter;

import io.r2dbc.spi.Row;

/**
 * Convertes an Object into another one.
 *
 * @see Row#get(int, Class)
 * @see Row#get(String, Class)
 * @author Thomas Freese
 * @param <T> Type
 */
public interface Converter<T>
{
    /**
     * @param value Object
     * @return Object
     */
    public T convert(Object value);

    /**
     * @return {@link Class}
     */
    public Class<T> getJavaType();
}
