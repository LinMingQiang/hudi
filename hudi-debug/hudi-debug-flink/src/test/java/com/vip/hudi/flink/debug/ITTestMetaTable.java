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

package com.vip.hudi.flink.debug;

import com.vip.hudi.flink.utils.TestUtil;

public class ITTestMetaTable extends TestUtil {
    /**
     * 这 meta table 到底是干嘛的 ：
     * https://www.cnblogs.com/leesf456/p/16990811.html
     * 1：目前使能了"hoodie.metadata.enable"后，会在.hoodie目录下生成一张名为metadata的mor表，利用该表可以显著提升源表的读写性能。
     *    该表目前包含三个分区：files, column_stats, bloom_filters，分区下文件的格式为hfile，采用该格式的目的主要是为了提高metadata表的点查能力。
     */



}
