package org.apache.aurora.scheduler.state.dsr;

public class DominantResourceType<T extends Number> {

    private final String resourceName;
    private final T resourceValue;

    public DominantResourceType(String resourceName, T resourceValue) {
        this.resourceName = resourceName;
        this.resourceValue = resourceValue;
    }

    public String getName() {
        return resourceName;
    }

    public T getValue() {
        return resourceValue;
    }
}
