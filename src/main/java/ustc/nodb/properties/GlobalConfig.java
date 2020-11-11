package ustc.nodb.properties;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class GlobalConfig {

    // Graph Sketch Config
    private static final byte hashNum;
    private static final int compressionRate;

    static {
        InputStream inputStream = GlobalConfig.class.getResourceAsStream("/project.properties");
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        hashNum = Byte.parseByte(properties.getProperty("hashNum", "1"));
        compressionRate = Integer.parseInt(properties.getProperty("compressionRate", "100"));
    }

    public static byte getHashNum() {
        return hashNum;
    }

    public static int getCompressionRate() {
        return compressionRate;
    }
}
