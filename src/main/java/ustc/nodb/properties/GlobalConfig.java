package ustc.nodb.properties;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class GlobalConfig {

    // Graph Sketch Config
    private static final byte hashNum;
    private static final short compressionRate;

    // Graph env config
    private static final String inputGraphPath;
    private static final int vCount;
    private static final int eCount;

    // Graph partition config
    private static final int partitionNum;

    // Graph cluster packing config
    private static final float alpha;

    static {
        InputStream inputStream = GlobalConfig.class.getResourceAsStream("/project.properties");
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        hashNum = Byte.parseByte(properties.getProperty("hashNum", "1"));
        compressionRate = Short.parseShort(properties.getProperty("compressionRate", "100"));
        inputGraphPath = properties.getProperty("inputGraphPath");
        vCount = Integer.parseInt(properties.getProperty("vCount"));
        eCount = Integer.parseInt(properties.getProperty("eCount"));
        partitionNum = Integer.parseInt(properties.getProperty("partitionNum"));
        alpha = Float.parseFloat(properties.getProperty("alpha"));
    }

    public static byte getHashNum() {
        return hashNum;
    }

    public static int getCompressionRate() {
        return compressionRate;
    }

    public static String getInputGraphPath() {
        return inputGraphPath;
    }

    public static int getVCount() {
        return vCount;
    }

    public static int getECount() {
        return eCount;
    }

    public static int getPartitionNum() {
        return partitionNum;
    }

    public static int getMaxClusterVolume() {
        return eCount / partitionNum;
    }

    public static float getAlpha() {
        return alpha;
    }
}
