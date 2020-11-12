package ustc.nodb.properties;

import org.junit.Test;

public class GlobalConfigTest {

    @Test
    public void testGetHashNum() {
        System.out.println(GlobalConfig.getHashNum() == 1);
    }

    @Test
    public void testGetCompressionRate() {
        GlobalConfig config;
        System.out.println(GlobalConfig.getCompressionRate() == 100);
    }

    @Test
    public void testGraphPath() {
        GlobalConfig config;
        System.out.println(GlobalConfig.getInputGraphPath());
    }
}