// Created: 14.06.2019
package io.r2dbc.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.junit.jupiter.api.Test;

/**
 * @author Thomas Freese
 */
final class JdbcDatabaseExceptionFactoryTest
{
    /**
     *
     */
    @Test
    void testFeatureNotSupportedException()
    {
        assertThat(JdbcR2dbcExceptionFactory.convert(new SQLFeatureNotSupportedException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcNonTransientException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void testIntegrityConstraintViolationException()
    {
        assertThat(JdbcR2dbcExceptionFactory.convert(new SQLIntegrityConstraintViolationException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcDataIntegrityViolationException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void testInvalidAuthorizationSpecException()
    {
        assertThat(JdbcR2dbcExceptionFactory.convert(new SQLInvalidAuthorizationSpecException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcPermissionDeniedException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void testNonTransientConnectionException()
    {
        assertThat(JdbcR2dbcExceptionFactory.convert(new SQLNonTransientConnectionException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcNonTransientResourceException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void testNonTransientException()
    {
        assertThat(JdbcR2dbcExceptionFactory.convert(new SQLNonTransientException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcNonTransientException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void testRecoverableException()
    {
        assertThat(JdbcR2dbcExceptionFactory.convert(new SQLRecoverableException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcNonTransientException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void testSqlDataException()
    {
        assertThat(JdbcR2dbcExceptionFactory.convert(new SQLDataException("SQLDataException", "SQLState", 999))).hasMessage("SQLDataException")
                .isInstanceOf(R2dbcException.class).extracting("sqlState", "errorCode").contains("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void testSqlException()
    {
        assertThat(JdbcR2dbcExceptionFactory.convert(new SQLException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void testSyntaxErrorException()
    {
        assertThat(JdbcR2dbcExceptionFactory.convert(new SQLSyntaxErrorException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcBadGrammarException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void testTimeoutException()
    {
        assertThat(JdbcR2dbcExceptionFactory.convert(new SQLTimeoutException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcTimeoutException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void testTransactionRollbackException()
    {
        assertThat(JdbcR2dbcExceptionFactory.convert(new SQLTransactionRollbackException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcRollbackException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void testTransientConnectionException()
    {
        assertThat(JdbcR2dbcExceptionFactory.convert(new SQLTransientConnectionException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcTransientResourceException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void testTransientException()
    {
        assertThat(JdbcR2dbcExceptionFactory.convert(new SQLTransientException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcTransientException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void testUnknownException()
    {
        assertThat(JdbcR2dbcExceptionFactory.convert(new SQLException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }
}
