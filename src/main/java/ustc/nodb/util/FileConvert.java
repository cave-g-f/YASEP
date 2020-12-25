package ustc.nodb.util;

import ustc.nodb.properties.GlobalConfig;

import java.io.*;
import java.util.Properties;

public class FileConvert {
    public static void main(String[] args) throws IOException {
        InputStream inputStream = FileConvert.class.getResourceAsStream("/amazon0302.txt");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        File file = new File("./adj.txt");
        if(!file.exists()) file.createNewFile();

        FileOutputStream fileOutputStream = new FileOutputStream(file);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));

        String line;
        int vertex = -1;
        while((line = bufferedReader.readLine()) != null){
            if (line.startsWith("#")) continue;
            String[] edgeValues = line.split("\t");
            int srcVid = Integer.parseInt(edgeValues[0]);

            if(vertex != srcVid){
                if (vertex != -1)  bufferedWriter.write("\n");
                bufferedWriter.write(Integer.toString(srcVid));
                vertex = srcVid;
            }

            bufferedWriter.write(" ");
            bufferedWriter.write(edgeValues[1]);
        }
    }
}
