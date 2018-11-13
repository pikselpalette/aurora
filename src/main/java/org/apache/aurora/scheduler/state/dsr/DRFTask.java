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
    private final Collection<DominantResourceType> resources;
    private final Double priority;

    public DRFTask(TaskGroupKey taskId, Collection<DominantResourceType> resources) {
        this.groupKey = taskId;
        this.resources = resources;
        this.priority = groupKey.getTask().getPriority() / 100.0;
    }

    public TaskGroupKey getGroupKey() {
        return groupKey;
    }

    public Collection<DominantResourceType> getResources() {
        return resources;
    }

    public Double getPriority() {
        return priority;
    }

    public DominantResource dominantResource(Collection<DominantResourceType> totalResources) {
        Map<String, Double> totalResourcesMap = resourceTypesCollectionsToMap(totalResources);
        Optional<DominantResource> dominantResource = getResources().stream()
                .map(resource -> buildDominantResource(resource.getName(), resource.getValue(), totalResourcesMap.get(resource.getName())))
                .max(Comparator.comparing(DominantResource::getPercentageUsage));

        if(!dominantResource.isPresent()) {
            LOG.warn("The resource types assigned to this task are not available: ", resources);
            return null;
        }
        return dominantResource.get();
    }

    public Double prioritizedDominantResource(Collection<DominantResourceType> totalResources) {
        DominantResource dominantResource = dominantResource(totalResources);
        if(dominantResource!=null) {
            return priority - dominantResource.getPercentageUsage();
        };
        return null;
    }

    @Override
    public String toString() {
        return "DRFTask{" +
                "groupKey=" + groupKey +
                ", resources=" + resources +
                ", priority=" + priority +
                '}';
    }
}
