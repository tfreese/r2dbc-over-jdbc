// Created: 25.03.2021
package io.r2dbc.jdbc;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.SuiteDisplayName;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/***
 * @author Thomas Freese
 */
@RunWith(Suite.class)
@SuiteDisplayName("r2dbc-over-jdbc Test Suite")
// @SelectPackages("io.r2dbc.jdbc")
@SelectClasses(
{
        ParameterizedRowTest.class, JdbcResultTest.class
})
// @IncludePackages
// @ExcludePackages
class TestSuite
{
    /**
     * Erstellt ein neues {@link TestSuite} Object.
     */
    private TestSuite()
    {
        super();
    }
}
