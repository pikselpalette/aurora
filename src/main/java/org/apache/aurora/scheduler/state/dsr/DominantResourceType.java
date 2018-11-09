package org.apache.aurora.scheduler.state.dsr;

public class DominantResourceType {

    private final String resourceName;
    private final Double resourceValue;

    public DominantResourceType(String resourceName, Double resourceValue) {
        this.resourceName = resourceName;
        this.resourceValue = resourceValue;
    }

    public String getName() {
        return resourceName;
    }

    public Double getValue() {
        return resourceValue;
    }

    @Override
    public String toString() {
        return "DominantResourceType{" +
                "resourceName='" + resourceName + '\'' +
                ", resourceValue=" + resourceValue +
                '}';
    }
}
