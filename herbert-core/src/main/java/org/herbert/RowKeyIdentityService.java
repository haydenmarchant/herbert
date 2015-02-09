package org.herbert;

/**
 * Created by hayden on 8/3/14.
 */
public class RowKeyIdentityService implements RowKeyService {
    @Override
    public byte[] generateHBaseKey(byte[] rawkey) {
        return rawkey;
    }

    @Override
    public byte[] getRawKey(byte[] hashedkey) {
        return hashedkey;
    }

    @Override
    public byte[] _getHashPrefix(byte[] rawkey) {
        return new byte[]{rawkey[0]};
    }
}
