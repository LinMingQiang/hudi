package com.vip.hudi.flink.mor.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Random;

public class Unuse {
    public static void main(String[] args) throws IOException {
        File f = new File("/Users/hunter/workspace/vip/hudi/hudi-examples/hudi-examples-debug/src/test/resources/testData");
        FileWriter s = new FileWriter(f);
        Random r = new Random();
        for (int i = 0; i < 10; i++) {
            s.write("id-"+r.nextInt(5)+",namex, "+i+",2022-01-0"+ (r.nextInt( 4)+1)+" 11:11:11");
            s.write("\n");
        }
        s.flush();
        s.close();
    }
}
