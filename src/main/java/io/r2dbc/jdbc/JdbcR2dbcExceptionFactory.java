/**
 * Created: 11.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.R2dbcRollbackException;
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.R2dbcTransientException;
import io.r2dbc.spi.R2dbcTransientResourceException;

/**
 * R2DBC Adapter for JDBC.
 * 
 * @author Thomas Freese
 */
public class JdbcR2dbcExceptionFactory
{
    /**
     * @author Thomas Freese
     */
    static class JdbcR2dbcDataException extends R2dbcException
    {
        /**
         *
         */
        private static final long serialVersionUID = -6441515280969268939L;

        /**
         * Erstellt ein neues {@link JdbcR2dbcDataException} Object.
         *
         * @param message String
         * @param sqlState String
         * @param errorCode int
         * @param e {@link SQLException}
         */
        JdbcR2dbcDataException(final String message, final String sqlState, final int errorCode, final SQLException e)
        {
            super(message, sqlState, errorCode, e);
        }
    }

    /**
     * @author Thomas Freese
     */
    static class JdbcR2dbcException extends R2dbcException
    {
        /**
         *
         */
        private static final long serialVersionUID = 6607810949597270120L;

        /**
         * Erstellt ein neues {@link JdbcR2dbcDataException} Object.
         *
         * @param message String
         * @param sqlState String
         * @param errorCode int
         * @param e {@link SQLException}
         */
        JdbcR2dbcException(final String message, final String sqlState, final int errorCode, final SQLException e)
        {
            super(message, sqlState, errorCode, e);
        }
    }

    /**
     * @author Thomas Freese
     */
    static class JdbcR2dbcNonTransientException extends R2dbcNonTransientException
    {
        /**
         *
         */
        private static final long serialVersionUID = -4703171992215253093L;

        /**
         * Erstellt ein neues {@link JdbcR2dbcDataException} Object.
         *
         * @param message String
         * @param sqlState String
         * @param errorCode int
         * @param e {@link SQLException}
         */
        JdbcR2dbcNonTransientException(final String message, final String sqlState, final int errorCode, final SQLException e)
        {
            super(message, sqlState, errorCode, e);
        }
    }

    /**
     * @author Thomas Freese
     */
    static class JdbcR2dbcTransientException extends R2dbcTransientException
    {
        /**
         *
         */
        private static final long serialVersionUID = -5814246921224867624L;

        /**
         * Erstellt ein neues {@link JdbcR2dbcDataException} Object.
         *
         * @param message String
         * @param sqlState String
         * @param errorCode int
         * @param e {@link SQLException}
         */
        JdbcR2dbcTransientException(final String message, final String sqlState, final int errorCode, final SQLException e)
        {
            super(message, sqlState, errorCode, e);
        }
    }

    /**
     * @param e {@link SQLException}
     * @return {@link R2dbcException}
     */
    public static R2dbcException create(final SQLException e)
    {
        if (e.getClass() == SQLDataException.class)
        {
            return new JdbcR2dbcDataException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }

        if (e.getClass() == SQLException.class)
        {
            return new JdbcR2dbcException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }

        if (e.getClass() == SQLFeatureNotSupportedException.class)
        {
            return new JdbcR2dbcNonTransientException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }

        if (e.getClass() == SQLIntegrityConstraintViolationException.class)
        {
            return new R2dbcDataIntegrityViolationException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }

        if (e.getClass() == SQLInvalidAuthorizationSpecException.class)
        {
            return new R2dbcPermissionDeniedException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }

        if (e.getClass() == SQLNonTransientConnectionException.class)
        {
            return new R2dbcNonTransientResourceException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }

        if (e.getClass() == SQLNonTransientException.class)
        {
            return new JdbcR2dbcNonTransientException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }

        if (e.getClass() == SQLRecoverableException.class)
        {
            return new JdbcR2dbcNonTransientException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }

        if (e.getClass() == SQLSyntaxErrorException.class)
        {
            return new R2dbcBadGrammarException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }

        if (e.getClass() == SQLTimeoutException.class)
        {
            return new R2dbcTimeoutException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }

        if (e.getClass() == SQLTransactionRollbackException.class)
        {
            return new R2dbcRollbackException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }

        if (e.getClass() == SQLTransientConnectionException.class)
        {
            return new R2dbcTransientResourceException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }

        if (e.getClass() == SQLTransientException.class)
        {
            return new JdbcR2dbcTransientException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }

        return new JdbcR2dbcException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
    }
}
