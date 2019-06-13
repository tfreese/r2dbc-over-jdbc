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
import org.junit.jupiter.api.Test;
import io.r2dbc.jdbc.JdbcR2dbcExceptionFactory;
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
 * @author Thomas Freese
 */
final class JdbcDatabaseExceptionFactoryTest
{
    /**
     *
     */
    @Test
    void featureNotSupportedException()
    {
        assertThat(JdbcR2dbcExceptionFactory.create(new SQLFeatureNotSupportedException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcNonTransientException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void integrityConstraintViolationException()
    {
        assertThat(JdbcR2dbcExceptionFactory.create(new SQLIntegrityConstraintViolationException("SQLException", "SQLState", 999)))
                .hasMessage("SQLException").isInstanceOf(R2dbcDataIntegrityViolationException.class).extracting("sqlState", "errorCode")
                .containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void invalidAuthorizationSpecException()
    {
        assertThat(JdbcR2dbcExceptionFactory.create(new SQLInvalidAuthorizationSpecException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcPermissionDeniedException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void nonTransientConnectionException()
    {
        assertThat(JdbcR2dbcExceptionFactory.create(new SQLNonTransientConnectionException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcNonTransientResourceException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void nonTransientException()
    {
        assertThat(JdbcR2dbcExceptionFactory.create(new SQLNonTransientException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcNonTransientException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void recoverableException()
    {
        assertThat(JdbcR2dbcExceptionFactory.create(new SQLRecoverableException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcNonTransientException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void sqlDataException()
    {
        assertThat(JdbcR2dbcExceptionFactory.create(new SQLDataException("SQLDataException", "SQLState", 999))).hasMessage("SQLDataException")
                .isInstanceOf(R2dbcException.class).extracting("sqlState", "errorCode").contains("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void sqlException()
    {
        assertThat(JdbcR2dbcExceptionFactory.create(new SQLException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void syntaxErrorException()
    {
        assertThat(JdbcR2dbcExceptionFactory.create(new SQLSyntaxErrorException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcBadGrammarException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void timeoutException()
    {
        assertThat(JdbcR2dbcExceptionFactory.create(new SQLTimeoutException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcTimeoutException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void transactionRollbackException()
    {
        assertThat(JdbcR2dbcExceptionFactory.create(new SQLTransactionRollbackException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcRollbackException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void transientConnectionException()
    {
        assertThat(JdbcR2dbcExceptionFactory.create(new SQLTransientConnectionException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcTransientResourceException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void transientException()
    {
        assertThat(JdbcR2dbcExceptionFactory.create(new SQLTransientException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcTransientException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }

    /**
     *
     */
    @Test
    void unknownException()
    {
        assertThat(JdbcR2dbcExceptionFactory.create(new SQLException("SQLException", "SQLState", 999))).hasMessage("SQLException")
                .isInstanceOf(R2dbcException.class).extracting("sqlState", "errorCode").containsExactly("SQLState", 999);
    }
}
