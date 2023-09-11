package org.jafardb;

public class Options {
    private int pageSize = 4096;
    private float minFillPercent = 0.0125F;
    private float maxFillPercent = 0.025F;

    public int getPageSize() {
        return pageSize;
    }
    public float getMinFillPercent() {
        return minFillPercent;
    }
    public float getMaxFillPercent() {
        return maxFillPercent;
    }

    public void setPageSize(int pageSize) { this.pageSize = pageSize; }

    public Options() {}

    public Options(int pageSize, float minFillPercent, float maxFillPercent) {
       this.pageSize = pageSize;
       this.minFillPercent = minFillPercent;
       this.maxFillPercent = maxFillPercent;
    }
}
