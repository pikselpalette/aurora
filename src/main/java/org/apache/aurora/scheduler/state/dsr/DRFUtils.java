package org.apache.aurora.scheduler.state.dsr;

import org.apache.aurora.scheduler.base.TaskGroupKey;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class DRFUtils {

    public static DRFTask buildDRFTask(TaskGroupKey taskGroupKey, double numCpus, long diskMb, long ramMb) {
        DominantResourceType<Double> cpu = new DominantResourceType<>("cpus", numCpus);
        DominantResourceType<Long> disk = new DominantResourceType<>("disk", diskMb);
        DominantResourceType<Long> ram = new DominantResourceType<>("mem", ramMb);

        return new DRFTask(taskGroupKey, Arrays.asList(cpu, ram, disk));
    }

    public static DominantResource buildDominantResource(String resourceName, Number value, Number totalAvailable) {
        Double percentage = value.doubleValue() / totalAvailable.doubleValue();
        return new DominantResource(resourceName, percentage.floatValue());
    }

    public static Map<String, Number> resourceTypesCollectionsToMap(Collection<DominantResourceType<Number>> resources) {
        return resources.stream().collect(Collectors.toMap(DominantResourceType::getName, DominantResourceType::getValue));
    }
}
