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
import org.apache.commons.lang3.StringUtils;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * @author huldarchen
 * @version 1.0
 * @date 2022/11/15 11:14
 */
public class WeekUtils {

    public static final String yyyyMMddPattern = "yyyyMMdd";
    public static final String yyyyMMPattern = "yyyyMM";
    public static final DateTimeFormatter yyyyMMddFormat = DateTimeFormat.forPattern(yyyyMMddPattern);

    private WeekUtils() {
    }

    /**
     * 对期周起始时间
     *
     * @param calcDateStr 要计算的天字符串
     */
    public static String lastYearWeekStart(String calcDateStr) {
        Preconditions.checkArgument(StringUtils.isNotBlank(calcDateStr), "时间不能为空");
        LocalDate curDate = LocalDate.parse(calcDateStr, yyyyMMddFormat);
        return lastYearWeek(curDate, 1).toString(yyyyMMddPattern);
    }

    /**
     * 对期周满周
     *
     * @param calcDateStr 要计算的天字符串
     */
    public static String lastYearWeekEndFull(String calcDateStr) {
        Preconditions.checkArgument(StringUtils.isNotBlank(calcDateStr), "时间不能为空");
        LocalDate curDate = LocalDate.parse(calcDateStr, yyyyMMddFormat);
        return lastYearWeek(curDate, 7).toString(yyyyMMddPattern);
    }

    /**
     * 对期周至今
     *
     * @param calcDateStr 要计算的天字符串
     */
    public static String lastYearWeekEnd(String calcDateStr) {
        Preconditions.checkArgument(StringUtils.isNotBlank(calcDateStr), "时间不能为空");
        LocalDate curDate = LocalDate.parse(calcDateStr, yyyyMMddFormat);
        int dayOfWeek = curDate.getDayOfWeek();
        return lastYearWeek(curDate, dayOfWeek).toString(yyyyMMddPattern);
    }

    /**
     * 对期周计算
     *
     * @param date      要计算的天
     * @param dayOfWeek 周几
     */
    public static LocalDate lastYearWeek(LocalDate date, int dayOfWeek) {
        Preconditions.checkArgument(date != null, "时间不能为空");
        Preconditions.checkArgument(dayOfWeek >= 1 && dayOfWeek <= 7, "周的第几天,从1到7");

        int weekOfWeekyear = date.getWeekOfWeekyear();
        int curWeekyear = date.getWeekyear();

        LocalDate lastWeekYear = date.withWeekyear(curWeekyear - 1);
        int lastWeekYearMaxValue = lastWeekYear.weekOfWeekyear().getMaximumValue();
        weekOfWeekyear = Math.min(weekOfWeekyear, lastWeekYearMaxValue);

        return lastWeekYear.withWeekOfWeekyear(weekOfWeekyear).withDayOfWeek(dayOfWeek);
    }

    /**
     * 环期周开始
     *
     * @param calcDateStr 要计算的天
     */
    public static String preWeekStart(String calcDateStr) {
        Preconditions.checkArgument(StringUtils.isNotBlank(calcDateStr), "时间不能为空");
        LocalDate curDate = LocalDate.parse(calcDateStr, yyyyMMddFormat);
        return preWeek(curDate, 1).toString(yyyyMMddPattern);
    }

    /**
     * 环期周结束(至计算天所在周几)
     *
     * @param calcDateStr 要计算的天
     */
    public static String preWeekEnd(String calcDateStr) {
        Preconditions.checkArgument(StringUtils.isNotBlank(calcDateStr), "时间不能为空");
        LocalDate curDate = LocalDate.parse(calcDateStr, yyyyMMddFormat);
        int dayOfWeek = curDate.getDayOfWeek();
        return preWeek(curDate, dayOfWeek).toString(yyyyMMddPattern);
    }

    /**
     * 环期周结束,满周
     *
     * @param calcDateStr 要计算的天
     */
    public static String preWeekEndFull(String calcDateStr) {
        Preconditions.checkArgument(StringUtils.isNotBlank(calcDateStr), "时间不能为空");
        LocalDate curDate = LocalDate.parse(calcDateStr, yyyyMMddFormat);
        return preWeek(curDate, 7).toString(yyyyMMddPattern);
    }

    /**
     * 环期周计算
     *
     * @param date      要计算的天
     * @param dayOfWeek 周几
     */
    public static LocalDate preWeek(LocalDate date, int dayOfWeek) {
        Preconditions.checkArgument(date != null, "时间不能为空");
        Preconditions.checkArgument(dayOfWeek >= 1 && dayOfWeek <= 7, "周的第几天,从1到7");
        return date.minusWeeks(1).withDayOfWeek(dayOfWeek);
    }
}
