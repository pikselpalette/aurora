package org.apache.aurora.scheduler.state.dsr;

import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.h2.value.ValueLob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class DRFUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DRFUtils.class);

    public static DRFTask buildDRFTask(TaskGroupKey taskGroupKey, double numCpus, long diskMb, long ramMb) {
        DominantResourceType cpu = new DominantResourceType("cpus", numCpus);
        DominantResourceType disk = new DominantResourceType("disk", Long.valueOf(diskMb).doubleValue());
        DominantResourceType ram = new DominantResourceType("mem", Long.valueOf(ramMb).doubleValue());

        return new DRFTask(taskGroupKey, Arrays.asList(cpu, ram, disk));
    }

    public static DominantResource buildDominantResource(String resourceName, Double value, Double totalAvailable) {
        Double percentage = value / totalAvailable;
        LOG.info("===================== VALUE, TOTAL, PERCENTAGE -->>>>>>>>>>>>>>>>>>> {}, {}, {}", value, totalAvailable, percentage);
        return new DominantResource(resourceName, percentage);
    }

    public static Map<String, Double> resourceTypesCollectionsToMap(Collection<DominantResourceType> resources) {
        return resources.stream()
                .collect(Collectors.toMap(DominantResourceType::getName, DominantResourceType::getValue, (oldV, newV) -> oldV + newV));
    }
}
