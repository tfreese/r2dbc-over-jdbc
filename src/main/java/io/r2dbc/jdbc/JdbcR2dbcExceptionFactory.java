// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.io.Serial;
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
public final class JdbcR2dbcExceptionFactory {
    /**
     * @author Thomas Freese
     */
    static class JdbcR2dbcDataException extends R2dbcException {
        @Serial
        private static final long serialVersionUID = -6441515280969268939L;

        JdbcR2dbcDataException(final String message, final String sqlState, final int errorCode, final SQLException cause) {
            super(message, sqlState, errorCode, cause);
        }
    }

    /**
     * @author Thomas Freese
     */
    static class JdbcR2dbcException extends R2dbcException {
        @Serial
        private static final long serialVersionUID = 6607810949597270120L;

        JdbcR2dbcException(final String message, final String sqlState, final int errorCode, final SQLException cause) {
            super(message, sqlState, errorCode, cause);
        }
    }

    /**
     * @author Thomas Freese
     */
    static class JdbcR2dbcNonTransientException extends R2dbcNonTransientException {
        @Serial
        private static final long serialVersionUID = -4703171992215253093L;

        JdbcR2dbcNonTransientException(final String message, final String sqlState, final int errorCode, final SQLException cause) {
            super(message, sqlState, errorCode, cause);
        }
    }

    /**
     * @author Thomas Freese
     */
    static class JdbcR2dbcTransientException extends R2dbcTransientException {
        @Serial
        private static final long serialVersionUID = -5814246921224867624L;

        JdbcR2dbcTransientException(final String message, final String sqlState, final int errorCode, final SQLException cause) {
            super(message, sqlState, errorCode, cause);
        }
    }

    public static R2dbcException convert(final SQLException ex) {
        if (ex.getClass() == SQLDataException.class) {
            return new JdbcR2dbcDataException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), ex);
        }

        if (ex.getClass() == SQLException.class) {
            return new JdbcR2dbcException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), ex);
        }

        if (ex.getClass() == SQLFeatureNotSupportedException.class) {
            return new JdbcR2dbcNonTransientException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), ex);
        }

        if (ex.getClass() == SQLIntegrityConstraintViolationException.class) {
            return new R2dbcDataIntegrityViolationException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), ex);
        }

        if (ex.getClass() == SQLInvalidAuthorizationSpecException.class) {
            return new R2dbcPermissionDeniedException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), ex);
        }

        if (ex.getClass() == SQLNonTransientConnectionException.class) {
            return new R2dbcNonTransientResourceException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), ex);
        }

        if (ex.getClass() == SQLNonTransientException.class) {
            return new JdbcR2dbcNonTransientException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), ex);
        }

        if (ex.getClass() == SQLRecoverableException.class) {
            return new JdbcR2dbcNonTransientException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), ex);
        }

        if (ex.getClass() == SQLSyntaxErrorException.class) {
            return new R2dbcBadGrammarException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), ex);
        }

        if (ex.getClass() == SQLTimeoutException.class) {
            return new R2dbcTimeoutException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), ex);
        }

        if (ex.getClass() == SQLTransactionRollbackException.class) {
            return new R2dbcRollbackException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), ex);
        }

        if (ex.getClass() == SQLTransientConnectionException.class) {
            return new R2dbcTransientResourceException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), ex);
        }

        if (ex.getClass() == SQLTransientException.class) {
            return new JdbcR2dbcTransientException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), ex);
        }

        return new JdbcR2dbcException(ex.getMessage(), ex.getSQLState(), ex.getErrorCode(), ex);
    }

    private JdbcR2dbcExceptionFactory() {
        super();
    }
}
