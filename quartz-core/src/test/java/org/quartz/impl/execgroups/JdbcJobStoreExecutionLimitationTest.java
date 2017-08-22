package org.quartz.impl.execgroups;

import org.quartz.impl.jdbcjobstore.JdbcQuartzTestUtilities;
import org.quartz.impl.jdbcjobstore.JobStoreTX;
import org.quartz.spi.JobStore;

import java.sql.SQLException;

/**
 * @author mederly (Evolveum)
 */
public class JdbcJobStoreExecutionLimitationTest extends AbstractExecutionLimitationTest {

    @Override
    protected void prepare(String testName) {
        try {
            JdbcQuartzTestUtilities.createDatabase(testName);
        } catch (SQLException e) {
            throw new AssertionError("Couldn't create database: " + testName, e);
        }
    }

    @Override
    protected JobStoreTX createJobStore(String testName) {
        JobStoreTX jobStore = new JobStoreTX();
        jobStore.setDataSource(testName);
        jobStore.setTriggerAcquisitionMaxFetchCount(1);     // this is actually the worst case
        return jobStore;
    }

    @Override
    protected JobStore createJobStore(String testName, long misfireThreshold) {
        JobStoreTX jobStore = createJobStore(testName);
        jobStore.setMisfireThreshold(misfireThreshold);
        return jobStore;
    }

    @Override
    protected void cleanup(String testName) {
        try {
            JdbcQuartzTestUtilities.destroyDatabase(testName);
        } catch (SQLException e) {
            throw new AssertionError("Couldn't destroy database: " + testName, e);
        }
    }

}
