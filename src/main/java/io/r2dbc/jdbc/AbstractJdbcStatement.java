/**
 * Created: 14.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.r2dbc.jdbc.codec.Codecs;
import io.r2dbc.jdbc.codec.encoder.Encoder;
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
    static class Bindings
    {
        /**
         *
         */
        private final List<Map<Integer, Object>> bindings = new ArrayList<>();

        /**
         *
         */
        private Map<Integer, Object> current;

        /**
         * Erstellt ein neues {@link Bindings} Object.
         */
        public Bindings()
        {
            super();
        }

        /**
         *
         */
        void finish()
        {
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
                this.bindings.add(this.current);
            }

            return this.current;
        }

        /**
         * @return
         */
        Map<Integer, Object> getLast()
        {
            if (this.bindings.isEmpty())
            {
                return getCurrent();
            }

            return this.bindings.get(this.bindings.size() - 1);
        }

        /**
         * @param preparedStatement {@link java.sql.PreparedStatement}
         * @throws java.sql.SQLException Falls was schief geht.
         */
        void prepareBatch(final java.sql.PreparedStatement preparedStatement) throws java.sql.SQLException
        {
            if (this.bindings.isEmpty())
            {
                preparedStatement.addBatch();
                return;
            }

            for (Map<Integer, Object> binding : this.bindings)
            {
                if (binding.isEmpty())
                {
                    continue;
                }

                prepareStatement(preparedStatement, binding);

                preparedStatement.addBatch();
            }
        }

        /**
         * @param preparedStatement {@link java.sql.PreparedStatement}
         * @param binding {@link Map}
         * @throws java.sql.SQLException Falls was schief geht.
         */
        void prepareStatement(final java.sql.PreparedStatement preparedStatement, final Map<Integer, Object> binding) throws java.sql.SQLException
        {
            for (Integer index : binding.keySet())
            {
                // JDBC fängt bei 1 an !
                int parameterIndex = index + 1;

                Object value = binding.get(index);

                if (value == null)
                {
                    preparedStatement.setObject(parameterIndex, null);
                }
                else
                {
                    Encoder<Object> encoder = Codecs.getEncoder(value.getClass());

                    encoder.encode(preparedStatement, parameterIndex, value);
                }
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
    static enum SQL_OPERATION
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
    private final java.sql.Connection connection;

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
     * @param connection {@link java.sql.Connection}
     * @param sql String
     */
    public AbstractJdbcStatement(final java.sql.Connection connection, final String sql)
    {
        super();

        this.connection = Objects.requireNonNull(connection, "connection must not be null");
        this.sql = Objects.requireNonNull(sql, "sql must not be null");

        // Determine SQL-Operation.
        String s = sql.substring(0, 6).toLowerCase();

        if (s.startsWith("select"))
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
        getBindings().getCurrent().put(index, value);

        return this;
    }

    /**
     * @see io.r2dbc.spi.Statement#bind(java.lang.Object, java.lang.Object)
     */
    @Override
    public Statement bind(final Object identifier, final Object value)
    {
        if (identifier instanceof Integer)
        {
            return bind(((Integer) identifier).intValue(), value);
        }
        else if (identifier instanceof String)
        {
            return bind(Integer.parseInt((String) identifier), value);
        }

        throw new IllegalArgumentException(
                String.format("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String represented integer.", identifier));
    }

    /**
     * @see io.r2dbc.spi.Statement#bindNull(int, java.lang.Class)
     */
    @Override
    public Statement bindNull(final int index, final Class<?> type)
    {
        getBindings().getCurrent().put(index, null);

        return this;
    }

    /**
     * @see io.r2dbc.spi.Statement#bindNull(java.lang.Object, java.lang.Class)
     */
    @Override
    public Statement bindNull(final Object identifier, final Class<?> type)
    {
        if (identifier instanceof Integer)
        {
            return bindNull(((Integer) identifier).intValue(), type);
        }
        else if (identifier instanceof String)
        {
            return bindNull(Integer.parseInt((String) identifier), type);
        }

        throw new IllegalArgumentException(
                String.format("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String represented integer.", identifier));
    }

    /**
     * @return {@link Bindings}
     */
    protected Bindings getBindings()
    {
        return this.bindings;
    }

    /**
     * @return {@link java.sql.Connection}
     */
    protected java.sql.Connection getConnection()
    {
        return this.connection;
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
