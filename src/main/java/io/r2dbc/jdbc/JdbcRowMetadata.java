// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.IntFunction;
import java.util.function.Predicate;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.RowMetadata;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public class JdbcRowMetadata implements RowMetadata
{
    /**
     * View of column metadata objects, with each object entry mapped to {@link ColumnMetadata#getName()}. In compliance with the {@link #getColumnNames()} SPI
     * method specification, this collection is unmodifiable, imposes the same column ordering as the column metadata objects list, and supports case
     * insensitive look ups.
     */
    private final class ColumnNamesCollection implements Collection<String>
    {
        /**
         * {@inheritDoc}
         * <p>
         * Prevents modification by throwing {@code UnsupportedOperationException}
         * </p>
         */
        @Override
        public boolean add(final String o)
        {
            throw modificationNotSupported();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Prevents modification by throwing {@code UnsupportedOperationException}
         * </p>
         */
        @Override
        public boolean addAll(final Collection<? extends String> c)
        {
            throw modificationNotSupported();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Prevents modification by throwing {@code UnsupportedOperationException}
         * </p>
         */
        @Override
        public void clear()
        {
            throw modificationNotSupported();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Returns true if the specified {@code name} is not null and is a String matching at least one column name, ignoring case.
         * </p>
         *
         * @param name A column name {@code String}, not null
         * @return True if a matching name is found
         * @throws NullPointerException If {@code name} is null
         * @throws ClassCastException If {@code name} is not a {@code String}
         */
        @Override
        public boolean contains(final Object name)
        {
            if (name == null)
            {
                throw new NullPointerException("Argument is null");
            }
            else if (!(name instanceof String))
            {
                throw new ClassCastException("Argument's type is not a String: " + name.getClass());
            }
            else
            {
                return JdbcRowMetadata.this.columnMetaDatasByName.containsKey(((String) name).toLowerCase());
            }
        }

        /**
         * {@inheritDoc}
         * <p>
         * Returns true if the case insensitive match implemented by {@link #contains(Object)} returns true for every element in the specified
         * {@code collection}.
         * </p>
         */
        @Override
        public boolean containsAll(final Collection<?> collection)
        {
            Objects.requireNonNull(collection, "collection is null");

            for (Object element : collection)
            {
                if (!contains(element))
                {
                    return false;
                }
            }

            return true;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Returns true if the column metadata object list is empty.
         * </p>
         */
        @Override
        public boolean isEmpty()
        {
            return JdbcRowMetadata.this.columnMetaDatas.isEmpty();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Returns an iterator over the list of column metadata which this row metadata object manages, with {@link Iterator#next()} mapping each metadata
         * object to its name.
         * </p>
         *
         * @implNote The returned iterator throws UnsupportedOperationException when {@link Iterator#remove()} is invoked.
         * @implNote The super class implements {@link #toArray()} and {@link #toArray(Object[])} by invoking this method.
         */
        @Override
        public Iterator<String> iterator()
        {
            return new Iterator<>()
            {
                /** Index of the next column metadata object which is iterated over */
                int index = 0;

                /**
                 * {@inheritDoc}
                 * <p>
                 * Returns {@code true} if the current {@link #index} is a valid index in the list of column metadata.
                 * </p>
                 */
                @Override
                public boolean hasNext()
                {
                    return this.index < JdbcRowMetadata.this.columnMetaDatas.size();
                }

                /**
                 * {@inheritDoc}
                 * <p>
                 * Returns the next column metadata object mapped to {@link ColumnMetadata#getName()}, and advances the {@link #index}.
                 * </p>
                 */
                @Override
                public String next()
                {
                    if (!hasNext())
                    {
                        throw new NoSuchElementException();
                    }

                    String name = JdbcRowMetadata.this.columnMetaDatas.get(this.index).getName();
                    this.index++;

                    return name;
                }
            };
        }

        /**
         * Returns an {@code UnsupportedOperationException} with a message indicating that {@link #getColumnNames()} returns a collection which is not
         * modifiable.
         *
         * @return A exception indicating that this collection is not modifiable.
         */
        private UnsupportedOperationException modificationNotSupported()
        {
            return new UnsupportedOperationException("The getColumnNames() Collection is unmodifiable");
        }

        /**
         * {@inheritDoc}
         * <p>
         * Prevents modification by throwing {@code UnsupportedOperationException}
         * </p>
         */
        @Override
        public boolean remove(final Object o)
        {
            throw modificationNotSupported();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Prevents modification by throwing {@code UnsupportedOperationException}
         * </p>
         */
        @Override
        public boolean removeAll(final Collection<?> c)
        {
            throw modificationNotSupported();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Prevents modification by throwing {@code UnsupportedOperationException}
         * </p>
         */
        @Override
        public boolean removeIf(final Predicate<? super String> filter)
        {
            throw modificationNotSupported();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Prevents modification by throwing {@code UnsupportedOperationException}
         * </p>
         */
        @Override
        public boolean retainAll(final Collection<?> c)
        {
            throw modificationNotSupported();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Returns the size of the column metadata list.
         * </p>
         *
         * @implNote The super class implements {@link AbstractCollection#isEmpty()} by invoking this method.
         */
        @Override
        public int size()
        {
            return JdbcRowMetadata.this.columnMetaDatas.size();
        }

        /**
         * {@inheritDoc}
         * <p>
         * Returns an array which stores the column names in the same order as the query result.
         * </p>
         */
        @Override
        public String[] toArray()
        {
            String[] namesArray = new String[JdbcRowMetadata.this.columnMetaDatas.size()];

            for (int i = 0; i < namesArray.length; i++)
            {
                namesArray[i] = JdbcRowMetadata.this.columnMetaDatas.get(i).getName();
            }

            return namesArray;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Returns an array allocated by the {@code generator}, which stores the column names in the same order as the query result.
         * </p>
         *
         * @implNote Unchecked class cast warnings are suppressed for casting the column name {@cod String} objects as {@code (T)} type objects which are stored
         *           in a {@code T[]} array. An {@code ArrayStoreException} results at runtime if {@code T} is not {@code String}.
         */
        @Override
        @SuppressWarnings("unchecked")
        public <T> T[] toArray(final IntFunction<T[]> generator)
        {
            Objects.requireNonNull(generator, "generator is null");

            T[] array = generator.apply(JdbcRowMetadata.this.columnMetaDatas.size());

            for (int i = 0; i < JdbcRowMetadata.this.columnMetaDatas.size(); i++)
            {
                array[i] = (T) JdbcRowMetadata.this.columnMetaDatas.get(i).getName();
            }

            return array;
        }

        /**
         * {@inheritDoc}
         * <p>
         * Returns an array which stores the column names in the same order as the query result.
         * </p>
         *
         * @implNote As specified by the {@code Collection} interface, the the {@code array} object is returned if it has capacity to store all column names. A
         *           null value is set after the last column name if the {@code array} capacity exceeds the number of column names. Otherwise, a newly allocated
         *           array is returned if the {@code array} does not have capacity to store all column names.
         * @implNote Unchecked class cast warnings are suppressed for invocations of {@link java.lang.reflect.Array#newInstance(Class, int)} which returns a
         *           {@code T[]} as {@code Object}.
         * @implNote Unchecked class cast warnings are suppressed for casting the column name {@cod String} objects as {@code (T)} type objects which are stored
         *           in a {@code T[]} array. An {@code ArrayStoreException} results at runtime if {@code T} is not {@code String}.
         */
        @Override
        @SuppressWarnings("unchecked")
        public <T> T[] toArray(T[] array)
        {
            Objects.requireNonNull(array, "array is null");

            int size = JdbcRowMetadata.this.columnMetaDatas.size();

            if (array.length < size)
            {
                array = (T[]) java.lang.reflect.Array.newInstance(array.getClass().getComponentType(), size);
            }
            else if (array.length > size)
            {
                array[size] = null;
            }

            for (int i = 0; i < size; i++)
            {
                array[i] = (T) JdbcRowMetadata.this.columnMetaDatas.get(i).getName();
            }

            return array;
        }
    }

    /**
     * @param resultSet {@link ResultSet}
     * @param codecs {@link Codecs}
     * @return {@link List}
     * @throws SQLException Falls was schief geht.
     */
    public static RowMetadata of(final ResultSet resultSet, final Codecs codecs) throws SQLException
    {
        if (resultSet == null)
        {
            return new JdbcRowMetadata(Collections.emptyList());
        }

        ResultSetMetaData metaData = resultSet.getMetaData();
        List<ColumnMetadata> list = new ArrayList<>();

        for (int column = 1; column <= metaData.getColumnCount(); column++)
        {
            String name = metaData.getColumnLabel(column);
            int sqlType = metaData.getColumnType(column);
            JDBCType jdbcType = JDBCType.valueOf(sqlType);
            int precision = metaData.getPrecision(column);
            int scale = metaData.getScale(column);

            Nullability nullability = switch (metaData.isNullable(column))
            {
                case ResultSetMetaData.columnNoNulls -> Nullability.NON_NULL;
                case ResultSetMetaData.columnNullable -> Nullability.NULLABLE;

                default -> Nullability.UNKNOWN;
            };

            Class<?> javaType = codecs.getJavaType(jdbcType);

            list.add(new JdbcColumnMetadata(name, column - 1, javaType, jdbcType, nullability, precision, scale));
        }

        return new JdbcRowMetadata(list);
    }

    /**
     *
     */
    private final List<ColumnMetadata> columnMetaDatas;

    /**
    *
    */
    private final Map<String, ColumnMetadata> columnMetaDatasByName = new LinkedHashMap<>();

    /**
     *
     */
    private final Collection<String> columnNames;

    /**
     * Erstellt ein neues {@link JdbcRowMetadata} Object.
     *
     * @param columnMetaDatas {@link ResultSet}
     */
    public JdbcRowMetadata(final List<ColumnMetadata> columnMetaDatas)
    {
        super();

        this.columnMetaDatas = Objects.requireNonNull(columnMetaDatas, "columnMetaDatas must not be null");

        columnMetaDatas.forEach(cmd -> {
            // Bei Spalten mit identischen Namen, immer den ersten nehmen, laut Spezifikation.
            String name = cmd.getName().toLowerCase();

            ColumnMetadata old = this.columnMetaDatasByName.put(name, cmd);

            if (old != null)
            {
                this.columnMetaDatasByName.put(name, old);
            }
        });

        this.columnNames = new ColumnNamesCollection();
    }

    /**
     * @see io.r2dbc.spi.RowMetadata#getColumnMetadata(int)
     */
    @Override
    public ColumnMetadata getColumnMetadata(final int index)
    {
        if ((index < 0) || (index >= this.columnMetaDatas.size()))
        {
            throw new ArrayIndexOutOfBoundsException("Index: " + index + ", Size: " + this.columnMetaDatas.size());
        }

        ColumnMetadata metaData = this.columnMetaDatas.get(index);

        if (metaData == null)
        {
            throw new NoSuchElementException("No MetaData for Index: " + index);
        }

        return metaData;
    }

    /**
     * @see io.r2dbc.spi.RowMetadata#getColumnMetadata(java.lang.String)
     */
    @Override
    public ColumnMetadata getColumnMetadata(final String name)
    {
        if (name == null)
        {
            throw new IllegalArgumentException("name is null");
        }

        ColumnMetadata metaData = this.columnMetaDatasByName.get(name.toLowerCase());

        if (metaData == null)
        {
            throw new NoSuchElementException("No MetaData for Name: " + name);
        }

        return metaData;
    }

    /**
     * @see io.r2dbc.spi.RowMetadata#getColumnMetadatas()
     */
    @Override
    public Iterable<ColumnMetadata> getColumnMetadatas()
    {
        return List.copyOf(this.columnMetaDatas);
    }

    /**
     * @see io.r2dbc.spi.RowMetadata#getColumnNames()
     */
    @Override
    public Collection<String> getColumnNames()
    {
        return this.columnNames;
    }
}
