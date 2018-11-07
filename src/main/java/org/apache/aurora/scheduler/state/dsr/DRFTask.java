package org.apache.aurora.scheduler.state.dsr;

import org.apache.aurora.scheduler.base.TaskGroupKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;

import static org.apache.aurora.scheduler.state.dsr.DRFUtils.buildDominantResource;
import static org.apache.aurora.scheduler.state.dsr.DRFUtils.resourceTypesCollectionsToMap;

public class DRFTask {

    private static final Logger LOG = LoggerFactory.getLogger(DRFTask.class);

    private final TaskGroupKey groupKey;
    private final Collection<DominantResourceType<?>> resources;
    private final Integer priority;

    public DRFTask(TaskGroupKey taskId, Collection<DominantResourceType<?>> resources) {
        this.groupKey = taskId;
        this.resources = resources;
        this.priority = groupKey.getTask().getPriority() / 100;
    }

    public TaskGroupKey getGroupKey() {
        return groupKey;
    }

    public Collection<DominantResourceType<?>> getResources() {
        return resources;
    }

    public Integer getPriority() {
        return priority;
    }

    public DominantResource dominantResource(Collection<DominantResourceType<Number>> totalResources) {
        Map<String, Number> totalResourcesMap = resourceTypesCollectionsToMap(totalResources);
        Optional<DominantResource> dominantResource = getResources().stream()
                .map(resource -> buildDominantResource(resource.getName(), resource.getValue(), totalResourcesMap.get(resource.getName())))
                .max(Comparator.comparing(DominantResource::getPercentageUsage));

        if(!dominantResource.isPresent()) {
            LOG.warn("The resource types assigned to this task are not available: ", resources);
            return null;
        }
        return dominantResource.get();
    }

    public Float prioritizedDominantResource(Collection<DominantResourceType<Number>> totalResources) {
        DominantResource dominantResource = dominantResource(totalResources);
        if(dominantResource!=null) {
            return priority.floatValue() - dominantResource(totalResources).getPercentageUsage();
        }
        return null;
    }
}
