package org.herbert;

import com.eaio.uuid.UUID;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.commons.math3.stat.descriptive.rank.Min;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class RowKeyHashingServiceTest {

    @Test
    public void testCalculateSplitKeysLessThant256_hashkey1() throws Exception {
        RowKeyHashingService presplitterService = new RowKeyHashingService(1, RowKeyHashingService.DEFAULT_HASH_RANGE);
        byte[][] splitkeys = presplitterService.calculateSplitKeys(4);
        assertEquals(4 * 4 - 1, splitkeys.length);
        assertNotSame((byte) 0, splitkeys[0][0]);

        assertEquals((byte) 16, splitkeys[0][0]);
        assertEquals((byte) 32, splitkeys[1][0]);
        assertEquals((byte) 48, splitkeys[2][0]);
        assertEquals((byte) 64, splitkeys[3][0]);
        assertEquals((byte) 80, splitkeys[4][0]);
        assertEquals((byte) 96, splitkeys[5][0]);
        assertEquals((byte) 112, splitkeys[6][0]);
        assertEquals((byte) 128, splitkeys[7][0]);
        assertEquals((byte) 144, splitkeys[8][0]);
        assertEquals((byte) 160, splitkeys[9][0]);
        assertEquals((byte) 176, splitkeys[10][0]);
        assertEquals((byte) 192, splitkeys[11][0]);
        assertEquals((byte) 208, splitkeys[12][0]);
        assertEquals((byte) 224, splitkeys[13][0]);
        assertEquals((byte) 240, splitkeys[14][0]);

        assertEquals(16, splitkeys[0][0]);
        assertEquals(1, splitkeys[0].length);

        assertEquals(1, splitkeys[10].length);

        assertNotSame((byte) 0, splitkeys[5][0]);
    }

    @Test
    public void testCalculateSplitKeysLessThan256_hashkey2() throws Exception {
        RowKeyHashingService presplitterService = new RowKeyHashingService(2, RowKeyHashingService.DEFAULT_HASH_RANGE);

        byte[][] splitkeys = presplitterService.calculateSplitKeys(4);
        assertEquals(4 * 4 - 1, splitkeys.length);
        assertNotSame((byte) 0, splitkeys[0][0]);

        assertEquals(2, splitkeys[0].length);

        assertEquals(2, splitkeys[10].length);

        assertNotSame((byte) 0, splitkeys[5][0]);
    }

    @Test
    public void testCalculateSplitKeysLessThan256_OddNumber() throws Exception {
        RowKeyHashingService rowKeyHashingService = new RowKeyHashingService(1, RowKeyHashingService.DEFAULT_HASH_RANGE);

        byte[][] splitkeys = rowKeyHashingService.calculateSplitKeys(7);
        assertEquals(4 * 7 - 1, splitkeys.length);

        assertTrue(isGapSame(splitkeys, 5, 18));
        assertTrue(isGapSame(splitkeys, 12, 24));

        assertEquals(1, splitkeys[0].length);

        assertEquals(1, splitkeys[10].length);

        assertNotSame(0, splitkeys[5][0]);
    }

    @Test
    public void testCalculateSplitKeysLargerThan256() throws Exception {
        RowKeyHashingService rowKeyHashingService = new RowKeyHashingService(2, RowKeyHashingService.DEFAULT_HASH_RANGE);

        byte[][] splitkeys = rowKeyHashingService.calculateSplitKeys(150);
        assertEquals(4 * 150 - 1, splitkeys.length);

        assertEquals(2, splitkeys[0].length);

        assertEquals(2, splitkeys[10].length);

        assertNotSame((byte) 0, splitkeys[5][0]);

        assertTrue(isGapSame(splitkeys, 5, 18));
        assertTrue(isGapSame(splitkeys, 12, 24));

      /*  for (int i = 0 ; i<splitkeys.length;i++) {
            byte[] splitkey = splitkeys[i];
            System.out.println(splitkey[0]+","+splitkey[1]);
        }*/

    }


    private int gap(byte[][] splitkeys, int index) {
        return splitkeys[index][0] - splitkeys[index - 1][0];
    }

    private boolean isGapSame(byte[][] splitkeys, int index1, int index2) {
        return Math.abs(gap(splitkeys, index1) - gap(splitkeys, index2)) <= 1;
    }

    @Test
    public void simpleHashkey() throws NoSuchAlgorithmException, DigestException {
        byte[] rawkey = {3, -5, 28, 10};

        RowKeyHashingService rowKeyHashingService = new RowKeyHashingService(2, RowKeyHashingService.DEFAULT_HASH_RANGE);

        byte[] hashed = rowKeyHashingService.generateHBaseKey(rawkey);
        assertNotNull(hashed);
        assertEquals(6, hashed.length);

        assertArrayEquals("last 4 bytes of hashed key should be same as raw", rawkey, Arrays.copyOfRange(hashed, 2, hashed.length));

        byte[] rawKey_reverse = rowKeyHashingService.getRawKey(hashed);
        assertEquals(4, rawKey_reverse.length);
        assertArrayEquals(rawkey, rawKey_reverse);
        RowKeyHashingService rowKeyHashingService2 = new RowKeyHashingService(2, RowKeyHashingService.DEFAULT_HASH_RANGE);

        //ensure that hashing is consistent
        assertArrayEquals(hashed, rowKeyHashingService2.generateHBaseKey(rawkey));

    }

    @Test
    public void humanReadable2Bytes() {
        RowKeyHashingService rowKeyHashingService = new RowKeyHashingService(2, RowKeyHashingService.HUMAN_READABLE_RANGE);
        byte[][] bytes = rowKeyHashingService.calculateSplitKeys(8);
        for (int i = 0; i < bytes.length; i++) {
            for (int j = 0; j < bytes[i].length; j++) {
                System.out.print((char) bytes[i][j]);
            }
            System.out.println();
        }
    }
    @Test
    public void numericsPresplitSingleByte() {
        RowKeyHashingService.HashRange hashRange = new RowKeyHashingService.HashRange('0', '9');

        RowKeyHashingService rowKeyHashingService = new RowKeyHashingService(2, hashRange);
        byte[][] bytes = rowKeyHashingService.calculateSplitKeys(4);
        System.out.println("bytes = " + bytes);

        String aid = new UUID().toString();
        byte[] rawkey = Bytes.toBytes(aid);
        byte[] hBaseKey = rowKeyHashingService.generateHBaseKey(rawkey);
        byte[] hbaseKeyStr = hBaseKey;
        System.out.println("Bytes.toString(rawkey) = " + Bytes.toString(rawkey));
        System.out.println("hbaseKeyStr = " + Bytes.toString(hbaseKeyStr));

        byte[] hashkey = rowKeyHashingService._getHashPrefix(rawkey);

        assertTrue("Hash must be in range defined: [" + hashkey[0] + "," + hashkey[1] + "]", hashkey[0] >= hashRange.getStart() && hashkey[0] <= hashRange.getEnd());
        assertTrue("Hash must be in range defined: [" + hashkey[0] + "," + hashkey[1] + "]", hashkey[1] >= hashRange.getStart() && hashkey[1] <= hashRange.getEnd());
    }

    @Test
    public void hashDistribution() throws IOException {
        RowKeyService rowKeyHashingService = RowKeyServiceFactory.createRowKeyService(2);
        checkDistribution(rowKeyHashingService);
    }

    @Test
    public void humanReadableDistribution() throws  IOException {
        RowKeyHashingService hrHashing = new RowKeyHashingService(2, RowKeyHashingService.HUMAN_READABLE_RANGE);
        checkDistribution(hrHashing);
    }
    @Test
    public void nullDistribution() throws IOException {
        RowKeyService rowKeyHashingService = RowKeyServiceFactory.createIndentityRowKeyService();
        checkDistribution(rowKeyHashingService);
    }

    private void checkDistribution(RowKeyService rowKeyHashingService) {
        Map<Byte, Integer> hashCount = new HashMap<Byte, Integer>();

        for (int i = 0; i < 1000000; i++) {
            hashKey(rowKeyHashingService, hashCount);
        }

        Collection<Integer> intValues = hashCount.values();
        double[] countsAsDbl = new double[intValues.size()];
        int i = 0;
        for (Integer next : intValues) {
            countsAsDbl[i++] = next;
//            System.out.println( next);
        }
        StandardDeviation standardDeviation = new StandardDeviation();
        double stdDev = standardDeviation.evaluate(countsAsDbl);
        System.out.println("Standard Deviation is: " + stdDev);

        double maxVal = new Max().evaluate(countsAsDbl);
        double minVal = new Min().evaluate(countsAsDbl);
        double skew = (maxVal - minVal) / (maxVal + minVal);
        System.out.println("Skew:" + skew);
    }

    private void hashKey(RowKeyService rowKeyHashingService, Map<Byte, Integer> hashCount) {
        byte[] rawkey = new UUID().toString().getBytes();
        byte[] hash = rowKeyHashingService.generateHBaseKey(rawkey);

        Integer count = hashCount.get(hash[0]);
        if (count == null) {
            count = 0;
        }
        count++;
        hashCount.put(hash[0], count);
    }
}