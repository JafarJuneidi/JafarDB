package org.example;

public class Options {
    private int pageSize = 4096;
    private float minFillPercent = (float) 0.0125;
    private float maxFillPercent = (float) 0.025;

    public int getPageSize() {
        return pageSize;
    }

    public float getMinFillPercent() {
        return minFillPercent;
    }

    public float getMaxFillPercent() {
        return maxFillPercent;
    }

    public Options() {}

    public Options(int pageSize, float minFillPercent, float maxFillPercent) {
       this.pageSize = pageSize;
       this.minFillPercent = minFillPercent;
       this.maxFillPercent = maxFillPercent;
    }
}
