package org.apache.aurora.scheduler.state.dsr;

public class DominantResource implements Comparable<DominantResource>{

    private final String name;
    private final Float percentageUsage;

    public DominantResource(String name, Float percentageUsage) {
        this.name = name;
        this.percentageUsage = percentageUsage;
    }

    public String getName() {
        return name;
    }

    public Float getPercentageUsage() {
        return percentageUsage;
    }

    @Override
    public int compareTo(DominantResource coming) {
        return percentageUsage.compareTo(coming.getPercentageUsage());
    }
}
