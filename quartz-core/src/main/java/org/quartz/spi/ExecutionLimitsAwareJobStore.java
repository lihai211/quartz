package org.quartz.spi;

import java.util.Map;

/**
 * @author mederly (Evolveum)
 */
public interface ExecutionLimitsAwareJobStore {

    void setExecutionLimits(Map<String, Integer> executionLimits);

}
