package org.apache.datasketches.characterization.filters;

import org.apache.datasketches.memory.WritableHandle;
import org.apache.datasketches.memory.WritableMemory;


import org.apache.datasketches.filters.quotientfilter.QuotientFilter;

public class QuotientFilterUpdateSpeedProfile extends BaseFilterUpdateSpeedProfile{
    protected QuotientFilter sketch;
    protected int lgNumSlots ;
    protected int numBitsPerSlot;
    private WritableHandle handle;
    private WritableMemory wmem;

    @Override
    public void configure() {
        lgNumSlots = Integer.parseInt(prop.mustGet("lgNumSlots"));
        numBitsPerSlot = Integer.parseInt(prop.mustGet("numBitsPerSlot"));
    }

    @Override
    public void cleanup() {
        try {
            if (handle != null) { handle.close(); }
        } catch (final Exception e) {}
    }

    @Override
    public double doTrial(final int uPerTrial) {
        //sketch.reset(); //is not implemented
        sketch = new QuotientFilter(lgNumSlots, numBitsPerSlot);
        final long startUpdateTime_nS = System.nanoTime();
        for (int u = uPerTrial; u-- > 0;) {
            sketch.insert(++vIn);
        }
        final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
        return (double) updateTime_nS / uPerTrial;
    }
}

