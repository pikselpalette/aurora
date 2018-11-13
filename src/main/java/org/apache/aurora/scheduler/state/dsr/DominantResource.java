package org.apache.aurora.scheduler.state.dsr;

public class DominantResource implements Comparable<DominantResource>{

    private final String name;
    private final Double percentageUsage;

    public DominantResource(String name, Double percentageUsage) {
        this.name = name;
        this.percentageUsage = percentageUsage;
    }

    public String getName() {
        return name;
    }

    public Double getPercentageUsage() {
        return percentageUsage;
    }

    @Override
    public int compareTo(DominantResource coming) {
        return percentageUsage.compareTo(coming.getPercentageUsage());
    }

    @Override
    public String toString() {
        return "DominantResource{" +
                "name='" + name + '\'' +
                ", percentageUsage=" + percentageUsage +
                '}';
    }
}
