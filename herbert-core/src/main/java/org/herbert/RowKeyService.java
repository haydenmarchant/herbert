package org.herbert;

/**
 * Created by hayden on 8/3/14.
 */
public interface RowKeyService {
     byte[] generateHBaseKey(byte[] rawkey);

    byte[] getRawKey(byte[] hashedkey);

    byte[] _getHashPrefix(byte[] rawkey);
}
