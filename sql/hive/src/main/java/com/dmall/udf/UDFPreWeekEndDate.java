/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package com.dmall.udf;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author huldarchen
 * @version 1.0
 * @date 2022/11/15 11:14
 */
public class UDFPreWeekEndDate extends UDF {
    private final Text result = new Text();

    /**
     * 计算对期月结束日期
     *
     * @param calcDateStr 日期
     * @return 返回结束日期
     */
    public Text evaluate(String calcDateStr) {
        return evaluate(calcDateStr, 1);
    }

    public Text evaluate(String calcDateStr, Integer type) {
        Preconditions.checkArgument(null != type, "时间不能为空");
        if (type == 2) {
            result.set(WeekUtils.preWeekEndFull(calcDateStr));
        } else {
            result.set(WeekUtils.preWeekEnd(calcDateStr));
        }

        return result;
    }
}
