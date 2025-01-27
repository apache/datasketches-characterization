package org.apache.datasketches.characterization.filters;

import org.apache.datasketches.filters.bloomfilter.BloomFilter;
import org.apache.datasketches.memory.WritableHandle;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.filters.bloomfilter.BloomFilterBuilder;

public class BloomFilterUpdateSpeedProfile extends BaseFilterUpdateSpeedProfile{
    protected BloomFilter sketch;
    private WritableHandle handle;
    private WritableMemory wmem;

    @Override
    public void configure() {
        //Configure Sketch
        final long numBits = Integer.parseInt(prop.mustGet("numBits"));
        final int numHashes = Integer.parseInt(prop.mustGet("numHashes"));
        sketch =  BloomFilterBuilder.createBySize(numBits, numHashes);
    }

    @Override
    public void cleanup() {
        try {
            if (handle != null) { handle.close(); }
        } catch (final Exception e) {}
    }

    @Override
    public double doTrial(final int uPerTrial) {
        sketch.reset();
        final long startUpdateTime_nS = System.nanoTime();

        for (int u = uPerTrial; u-- > 0;) {
            sketch.update(++vIn);
        }
        final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
        return (double) updateTime_nS / uPerTrial;
    }
}

