package org.herbert;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by hayden on 7/31/14.
 */
public class RowKeyHashingService implements RowKeyService,Serializable {

    private final static int INIT_REGIONS_PER_REGION_SERVER = 4;
    private static final int BYTE_SIZE = 256;
    public static final int NOT_SET = -1;
    private int totalNumRegions;
    private int hashLength = NOT_SET;
    private HashRange hashRange;
    private static final int JITTER_FACTOR = 5;
    private JitterGen jitterGenerator = new JitterGen(JITTER_FACTOR);
    public static class HashRange implements Serializable {
        private int start;
        private int end;

        public HashRange(int start, int end) {
            this.start = start;
            this.end = end;
        }

        int length() {
            return this.end - this.start + 1;
        }

        public int getStart() {
            return start;
        }

        public int getEnd() {
            return end;
        }
    }

    public RowKeyHashingService(int hashLength,HashRange hashRange) {
        this.hashLength = hashLength;

        if (hashRange == null) {
            hashRange = DEFAULT_HASH_RANGE;
        }
        this.hashRange = hashRange;
    }

    public static HashRange DEFAULT_HASH_RANGE = new HashRange(0,BYTE_SIZE - 1);
    public static HashRange HUMAN_READABLE_RANGE = new HashRange( 'A','z');

    public byte[][] calculateSplitKeys(HBaseAdmin hBaseAdmin) throws IOException {
        ClusterStatus clusterStatus = hBaseAdmin.getClusterStatus();
        int serversSize = clusterStatus.getServersSize();
        return calculateSplitKeys(serversSize);
    }


    public byte[][] calculateSplitKeys(int numRegionServers) {
        this.totalNumRegions = numRegionServers * INIT_REGIONS_PER_REGION_SERVER;
        int hashLength = this.hashRange.length();
        assert totalNumRegions <= hashLength * hashLength;

        int numBytes = getHashSize();

        byte[][] splitkeys = new byte[totalNumRegions - 1][numBytes];
        int jump = hashLength * hashLength / totalNumRegions;
        for (int i = 0; i < totalNumRegions - 1; i++) {
            if (numBytes == 1) {
                int byteChunk = (i + 1) * hashLength;
                int splitkey = this.hashRange.start + byteChunk / totalNumRegions;
                splitkeys[i][0] = (byte) splitkey;
            } else {
                int thisJump = jitter(jump);
                short sh = (short) ( i * jump + thisJump);
                byte byte1 = (byte) (this.hashRange.start + sh/ hashLength);
                byte byte2 = (byte) (this.hashRange.start + sh % hashLength);

                splitkeys[i] = new byte[] {byte1, byte2};
            }

        }
        return splitkeys;
    }

    private int jitter(int orig) {
        return jitterGenerator.jitter(orig);
    }

    public static class JitterGen implements Serializable {
        private int factor;
        private Random random;

        public JitterGen(int factor) {
            this.factor = Integer.getInteger("jitter",factor);
            this.random = new Random(System.currentTimeMillis());
        }

        public int jitter(int base) {
            int baseJitter = (base * factor) /100;
           return base + (int) Math.round (baseJitter  * (random.nextDouble() - 0.5));
        }

    }

    @Override
    public byte[] generateHBaseKey(byte[] rawkey) {
        byte[] hash = getHash(rawkey);

        byte[] hashedkey = new byte[hash.length + rawkey.length];
        System.arraycopy(rawkey, 0, hashedkey, hash.length, rawkey.length);
        System.arraycopy(hash, 0, hashedkey, 0, hash.length);

        return hashedkey;
    }

    @Override
    public byte[] getRawKey(byte[] hashedkey) {
        byte[] rawkey = new byte[hashedkey.length - this.hashLength];
        System.arraycopy(hashedkey, this.hashLength, rawkey, 0, rawkey.length);
        return rawkey;
    }

    public int getHashSize() {
        return hashLength;
    }

   /* private int calculateMinimumNumBytes() {
        int numBytes;
        if (totalNumRegions > BYTE_SIZE) {
            numBytes = 2;
        } else {
            numBytes = 1;
        }
        return numBytes;
    }*/

    public static  void main(String args[]) {
        byte[] aid = Bytes.toBytes(args[0]);
        RowKeyHashingService rowKeyHashingService = new RowKeyHashingService(2, HUMAN_READABLE_RANGE);
        byte[] hBaseKey = rowKeyHashingService.generateHBaseKey(aid);
        System.out.println("Bytes.toString(hBaseKey) = " + Bytes.toString(hBaseKey));

    }

    private byte[] getHash(byte[] raw) {
        //get 2 byte hash code from raw key (short == 2 bytes)
        int hashLength = this.hashRange.length();
        int rawHashCode = Arrays.hashCode(raw);
        if (rawHashCode < 0) {
            rawHashCode = Integer.MAX_VALUE + rawHashCode;
        }
        int hc = rawHashCode % (hashLength * hashLength);

        byte byte2 = (byte) (this.hashRange.start + hc % hashLength);
        if (this.hashLength == 1) {
            return new byte[]{byte2};
        } else if (this.hashLength == 2) {
            byte byte1 = (byte) (this.hashRange.start + (hc / hashLength));
            return new byte[]{byte1, byte2};
        } else {
            assert false;
            return null;
        }
    }

    @Override
    public byte[] _getHashPrefix(byte[] rawkey) {
        return getHash(rawkey);
    }
}
