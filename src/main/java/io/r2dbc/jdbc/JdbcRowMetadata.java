/**
 * Created: 12.06.2019
 */

package io.r2dbc.jdbc;

import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.RowMetadata;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public class JdbcRowMetadata implements RowMetadata
{
    /**
     * @param resultSet {@link ResultSet}
     *
     * @return {@link List}
     *
     * @throws SQLException Falls was schief geht.
     */
    private static List<JdbcColumnMetadata> extractColumnMetaData(final ResultSet resultSet) throws SQLException
    {
        ResultSetMetaData metaData = resultSet.getMetaData();
        List<JdbcColumnMetadata> list = new ArrayList<>();

        for (int c = 1; c <= metaData.getColumnCount(); c++)
        {
            String name = metaData.getColumnLabel(c).toUpperCase();
            int sqlType = metaData.getColumnType(c);
            Nullability nullability = null;

            switch (metaData.isNullable(c))
            {
                case ResultSetMetaData.columnNoNulls:
                    nullability = Nullability.NON_NULL;
                    break;

                case ResultSetMetaData.columnNullable:
                    nullability = Nullability.NULLABLE;
                    break;

                default:
                    nullability = Nullability.UNKNOWN;
                    break;
            }

            int precision = metaData.getPrecision(c);
            int scale = metaData.getScale(c);

            list.add(new JdbcColumnMetadata(name, sqlType, nullability, precision, scale));
        }

        return list;
    }

    /**
     *
     */
    private final Map<String, JdbcColumnMetadata> columnMetaDataByName;

    /**
     *
     */
    private final List<JdbcColumnMetadata> columnMetaDatas;

    /**
     * Erstellt ein neues {@link JdbcRowMetadata} Object.
     *
     * @param resultSet {@link ResultSet}
     *
     * @throws SQLException Falls was schief geht.
     */
    public JdbcRowMetadata(final ResultSet resultSet) throws SQLException
    {
        super();

        Objects.requireNonNull(resultSet, "resultSet must not be null");

        this.columnMetaDatas = extractColumnMetaData(resultSet);

        this.columnMetaDataByName = this.columnMetaDatas.stream()
                .collect(Collectors.toMap(cmd -> cmd.getName().toUpperCase(), Function.identity(), (a, b) -> a, LinkedHashMap::new));
    }

    /**
     * @see io.r2dbc.spi.RowMetadata#getColumnMetadata(java.lang.Object)
     */
    @Override
    public JdbcColumnMetadata getColumnMetadata(final Object identifier)
    {
        Objects.requireNonNull(identifier, "identifier must not be null");

        if (identifier instanceof Integer)
        {
            return this.columnMetaDatas.get((Integer) identifier);
        }
        else if (identifier instanceof String)
        {
            return this.columnMetaDataByName.get(((String) identifier).toUpperCase());
        }

        throw new IllegalArgumentException(
                String.format("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String column name.", identifier));
    }

    /**
     * @see io.r2dbc.spi.RowMetadata#getColumnMetadatas()
     */
    @Override
    public List<JdbcColumnMetadata> getColumnMetadatas()
    {
        return List.copyOf(this.columnMetaDatas);
    }

    /**
     * @see io.r2dbc.spi.RowMetadata#getColumnNames()
     */
    @Override
    public Collection<String> getColumnNames()
    {
        // return this.columnMetaDatas.stream().map(ColumnMetadata::getName).collect(Collectors.toList());
        return new LinkedHashSet<>(this.columnMetaDataByName.keySet());
    }
}
