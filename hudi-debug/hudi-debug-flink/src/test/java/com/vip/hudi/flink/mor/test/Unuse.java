/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
