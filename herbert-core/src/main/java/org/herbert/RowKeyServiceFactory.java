package org.herbert;

/**
 * Created by hayden on 8/3/14.
 */
public class RowKeyServiceFactory {
    public static RowKeyService createRowKeyService(int hashLength)  {
        return new RowKeyHashingService(hashLength,null);
    }

    public static RowKeyService createIndentityRowKeyService()  {
        return new RowKeyIdentityService();
    }

    public static RowKeyService createRowKeyService()  {
        return  new RowKeyHashingService(2,RowKeyHashingService.HUMAN_READABLE_RANGE);
    }
}
