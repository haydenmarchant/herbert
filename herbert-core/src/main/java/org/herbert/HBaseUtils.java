package org.herbert;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: yulia
 * Date: 6/9/14
 * Time: 11:36 AM
 */
public class HBaseUtils {
    protected static final Logger LOG = LoggerFactory.getLogger(HBaseUtils.class);


    public static byte[] serialize(long num) {
        return ByteBuffer.allocate(8).putLong(num).array();
    }

    public static long deserialize(byte[] num) {
        ByteBuffer buf = ByteBuffer.allocate(8).put(num);
        buf.flip();
        return buf.getLong();
    }

    public static String convertScanToString(Scan scan) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            scan.write(dos);
            return Base64.encodeBytes(out.toByteArray());
        } catch (IOException e) {
            LOG.error("Error in convertToString", e);
            return "";
        }

    }

    //AgAAAAAAAf//////////AQE2b3JnLmFwYWNoZS5oYWRvb3AuaGJhc2UuZmlsdGVyLlNpbmdsZUNv bHVtblZhbHVlRmlsdGVyAmNmC2xhc3RfdXBkYXRlABBHUkVBVEVSX09SX0VRVUFMNi8IAAABS1L1 kIAAAAEBAAAAAAAAAAB//////////wEAAAAAAAAAAA==
    public static Scan convertStringToScan(String str) {
        try {
            byte[] decodedBytes = Base64.decode(str);
            ByteArrayInputStream in = new ByteArrayInputStream(decodedBytes);
            DataInputStream dos = new DataInputStream(in);
            Scan scan = new Scan();
            scan.readFields(dos);
            return scan;
        } catch (IOException e) {
            LOG.error("Error in convertToString", e);
            return null;
        }

    }

    public static void main(String[] args) {
        Scan scan = convertStringToScan("AgFhAWIAAAAB//////////8BATZvcmcuYXBhY2hlLmhhZG9vcC5oYmFzZS5maWx0ZXIuU2luZ2xl Q29sdW1uVmFsdWVGaWx0ZXICY2YLbGFzdF91cGRhdGUAEEdSRUFURVJfT1JfRVFVQUw2LwgAAAFL Z48AgAAAAQEAAAAAAAAAAH//////////AQAAAAAAAAAA");
        System.out.println("scan = " + scan);
    }

//    public static byte[] serialize(Object obj) throws IOException {
//
//        ByteArrayOutputStream b = new ByteArrayOutputStream();
//        ObjectOutputStream o = new ObjectOutputStream(b);
//        o.writeObject(obj);
//        return b.toByteArray();
//    }
//    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
//        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
//        ObjectInputStream o = new ObjectInputStream(b);
//        return o.readObject();
//    }
//
//    public static String getString(byte[] bytes) {
//        return Bytes.toString(bytes);
//    }
}
