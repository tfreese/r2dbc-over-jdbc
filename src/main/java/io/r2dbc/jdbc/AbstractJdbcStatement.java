// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.spi.Statement;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public abstract class AbstractJdbcStatement implements Statement
{
    /**
     * @author Thomas Freese
     */
    class Bindings
    {
        /**
         *
         */
        private final List<Map<Integer, Object>> binds = new ArrayList<>();
        /**
         *
         */
        private Map<Integer, Object> current;

        /**
         *
         */
        void finish()
        {
            validateBinds();

            this.current = null;
        }

        /**
         * @return {@link Map}
         */
        Map<Integer, Object> getCurrent()
        {
            if (this.current == null)
            {
                this.current = new HashMap<>();
                this.binds.add(this.current);
            }

            return this.current;
        }

        /**
         * @return {@link Map}
         */
        Map<Integer, Object> getLast()
        {
            if (this.binds.isEmpty())
            {
                return getCurrent();
            }

            return this.binds.get(this.binds.size() - 1);
        }

        /**
         * @param preparedStatement {@link java.sql.PreparedStatement}
         *
         * @throws SQLException Falls was schief geht.
         */
        void prepareBatch(final PreparedStatement preparedStatement) throws SQLException
        {
            if (this.binds.isEmpty())
            {
                preparedStatement.addBatch();

                return;
            }

            for (Map<Integer, Object> bind : this.binds)
            {
                if (bind.isEmpty())
                {
                    continue;
                }

                prepareStatement(preparedStatement, bind);

                preparedStatement.addBatch();
            }
        }

        /**
         * @param preparedStatement {@link java.sql.PreparedStatement}
         * @param bind {@link Map}
         *
         * @throws SQLException Falls was schief geht.
         */
        void prepareStatement(final PreparedStatement preparedStatement, final Map<Integer, Object> bind) throws SQLException
        {
            for (Entry<Integer, Object> entry : bind.entrySet())
            {
                Integer index = entry.getKey();
                Object value = entry.getValue();

                // JDBC fängt bei 1 an !
                int parameterIndex = index + 1;

                if (value == null)
                {
                    // preparedStatement.setNull(parameterIndex, sqlType);
                    preparedStatement.setObject(parameterIndex, null);
                }
                else
                {
                    getCodecs().mapToSql(value.getClass(), preparedStatement, parameterIndex, value);
                }
            }
        }

        /**
         * Prüfen, ob alle Parameter gesetzt wurden.
         */
        void validateBinds()
        {
            if (this.current == null)
            {
                return;
            }

            long parameterCount = getSql().chars().filter(ch -> ch == '?').count();

            int bindCount = this.current.size();

            if (bindCount < parameterCount)
            {
                throw new IllegalStateException("Bindings do not match Parameters: " + bindCount + " != " + parameterCount);
            }
        }
    }

    /**
     * @author Thomas Freese
     */
    static class Context
    {
        /**
         *
         */
        private final int[] affectedRows;
        /**
         *
         */
        private final ResultSet resultSet;
        /**
         *
         */
        private final PreparedStatement stmt;

        /**
         * Erstellt ein neues {@link Context} Object.
         *
         * @param stmt {@link PreparedStatement}
         * @param resultSet {@link ResultSet}
         * @param affectedRows int[]
         */
        public Context(final PreparedStatement stmt, final ResultSet resultSet, final int[] affectedRows)
        {
            super();

            this.stmt = Objects.requireNonNull(stmt, "stmt must not be null");
            this.resultSet = resultSet;
            this.affectedRows = affectedRows;
        }

        /**
         * @return int[]
         */
        int[] getAffectedRows()
        {
            return this.affectedRows;
        }

        /**
         * @return {@link ResultSet}
         */
        ResultSet getResultSet()
        {
            return this.resultSet;
        }

        /**
         * @return {@link PreparedStatement}
         */
        PreparedStatement getStmt()
        {
            return this.stmt;
        }
    }

    /**
     * @author Thomas Freese
     */
    enum SQL_OPERATION
    {
        /**
         *
         */
        DELETE,
        /**
         *
         */
        EXECUTE,
        /**
         *
         */
        INSERT,
        /**
         *
         */
        SELECT,
        /**
         *
         */
        UPDATE;
    }

    /**
     *
     */
    private final Bindings bindings = new Bindings();
    /**
    *
    */
    private final Codecs codecs;
    /**
     *
     */
    private final java.sql.Connection jdbcConnection;
    /**
     *
     */
    private final Logger logger = LoggerFactory.getLogger(getClass());
    /**
     *
     */
    private final String sql;
    /**
     *
     */
    private final SQL_OPERATION sqlOperation;

    /**
     * Erstellt ein neues {@link AbstractJdbcStatement} Object.
     *
     * @param jdbcConnection {@link java.sql.Connection}
     * @param sql String
     * @param codecs {@link Codecs}
     */
    protected AbstractJdbcStatement(final java.sql.Connection jdbcConnection, final String sql, final Codecs codecs)
    {
        super();

        this.jdbcConnection = Objects.requireNonNull(jdbcConnection, "jdbcConnection required");
        this.sql = Objects.requireNonNull(sql, "sql required");
        this.codecs = Objects.requireNonNull(codecs, "codecs required");

        // Determine SQL-Operation.
        String s = sql.substring(0, 6).toLowerCase();

        if (s.startsWith("select") || s.startsWith("with"))
        {
            this.sqlOperation = SQL_OPERATION.SELECT;
        }
        else if (s.startsWith("delete"))
        {
            this.sqlOperation = SQL_OPERATION.DELETE;
        }
        else if (s.startsWith("update"))
        {
            this.sqlOperation = SQL_OPERATION.UPDATE;
        }
        else if (s.startsWith("insert"))
        {
            this.sqlOperation = SQL_OPERATION.INSERT;
        }
        else
        {
            this.sqlOperation = SQL_OPERATION.EXECUTE;
        }
    }

    /**
     * @see io.r2dbc.spi.Statement#add()
     */
    @Override
    public Statement add()
    {
        getBindings().finish();

        return this;
    }

    /**
     * @see io.r2dbc.spi.Statement#bind(int, java.lang.Object)
     */
    @Override
    public Statement bind(final int index, final Object value)
    {
        requireValidIndex(index);

        if (value == null)
        {
            throw new IllegalArgumentException("value is null");
        }

        if (Class.class.equals(value))
        {
            throw new IllegalArgumentException("value is type of class");
        }

        getBindings().getCurrent().put(index, value);

        return this;
    }

    /**
     * @see io.r2dbc.spi.Statement#bind(java.lang.String, java.lang.Object)
     */
    @Override
    public Statement bind(final String name, final Object value)
    {
        if (name == null)
        {
            throw new IllegalArgumentException("name is null");
        }

        if (value == null)
        {
            throw new IllegalArgumentException("value is null");
        }

        try
        {
            return bind(Integer.parseInt(name), value);
        }
        catch (Exception ex)
        {
            throw new IllegalArgumentException(
                    String.format("Name '%s' is not valid. Should either be an Integer index or a String represented integer.", name));
        }
    }

    /**
     * @see io.r2dbc.spi.Statement#bindNull(int, java.lang.Class)
     */
    @Override
    public Statement bindNull(final int index, final Class<?> type)
    {
        requireValidIndex(index);

        if (type == null)
        {
            throw new IllegalArgumentException("type is null");
        }

        getBindings().getCurrent().put(index, null);

        return this;
    }

    // /**
    // * @see io.r2dbc.spi.Statement#bind(java.lang.Object, java.lang.Object)
    // */
    // @Override
    // public Statement bind(final Object identifier, final Object value)
    // {
    // if (identifier instanceof Integer)
    // {
    // return bind(((Integer) identifier).intValue(), value);
    // }
    // else if (identifier instanceof String)
    // {
    // return bind(Integer.parseInt((String) identifier), value);
    // }
    //
    // throw new IllegalArgumentException(
    // String.format("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String represented integer.", identifier));
    // }

    /**
     * @see io.r2dbc.spi.Statement#bindNull(java.lang.String, java.lang.Class)
     */
    @Override
    public Statement bindNull(final String name, final Class<?> type)
    {
        if (name == null)
        {
            throw new IllegalArgumentException("name is null");
        }

        if (type == null)
        {
            throw new IllegalArgumentException("type is null");
        }

        try
        {
            return bind(Integer.parseInt(name), type);
        }
        catch (Exception ex)
        {
            throw new IllegalArgumentException(
                    String.format("Name '%s' is not valid. Should either be an Integer index or a String represented integer.", name));
        }
    }

    // /**
    // * @see io.r2dbc.spi.Statement#bindNull(java.lang.Object, java.lang.Class)
    // */
    // @Override
    // public Statement bindNull(final Object identifier, final Class<?> type)
    // {
    // if (identifier instanceof Integer)
    // {
    // return bindNull(((Integer) identifier).intValue(), type);
    // }
    // else if (identifier instanceof String)
    // {
    // return bindNull(Integer.parseInt((String) identifier), type);
    // }
    //
    // throw new IllegalArgumentException(
    // String.format("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String represented integer.", identifier));
    // }

    /**
     * @return {@link Bindings}
     */
    protected Bindings getBindings()
    {
        return this.bindings;
    }

    /**
     * @return {@link Codecs}
     */
    protected Codecs getCodecs()
    {
        return this.codecs;
    }

    /**
     * @return {@link java.sql.Connection}
     */
    protected java.sql.Connection getJdbcConnection()
    {
        return this.jdbcConnection;
    }

    /**
     * @return {@link Logger}
     */
    protected Logger getLogger()
    {
        return this.logger;
    }

    /**
     * @return String
     */
    protected String getSql()
    {
        return this.sql;
    }

    /**
     * @return {@link SQL_OPERATION}
     */
    protected SQL_OPERATION getSqlOperation()
    {
        return this.sqlOperation;
    }

    /**
     * Checks that the specified 0-based {@code index} is within the range of valid parameter indexes for this statement.
     *
     * @param index A 0-based parameter index
     *
     * @throws IndexOutOfBoundsException If the {@code index} is outside of the valid range.
     */
    protected void requireValidIndex(final int index)
    {
        if ((index < 0) || (index > this.bindings.binds.size()))
        {
            throw new IndexOutOfBoundsException("Parameter index is non-positive: " + index);
        }
    }

    // /**
    // * Beim DELETE hat das Array nur ein Element pro Batch mit der Anzahl gelöschter Zeilen.<br>
    // * Dies muss für das reaktive Verhalten auf n-Elemente erweitert werden.<br>
    // * [2, 1] -> [1, 1, 1]
    // *
    // * @param affectedRows int[]
    // * @return int[]
    // * @see JdbcPreparedStatementDeleteOld
    // */
    // protected int[] normalizeAffectedRowsForReactive(final int[] affectedRows)
    // {
    // // return affectedRows;
    // // int[] rows = IntStream.of(affectedRows).flatMap(ar -> {
    // // int[] r = new int[ar];
    // // Arrays.fill(r, 1);
    // // return IntStream.of(r);
    // // }).toArray();
    //
    // int[] rows = new int[IntStream.of(affectedRows).sum()];
    // Arrays.fill(rows, 1);
    //
    // return rows;
    // }

    // /**
    // * @see io.r2dbc.spi.Statement#returnGeneratedValues(java.lang.String[])
    // */
    // @Override
    // public Statement returnGeneratedValues(final String...columns)
    // {
    // throw new UnsupportedOperationException();
    // }
}
