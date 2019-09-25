package com.yaxin.release.enums;

/**
 * 年龄段
 */
public enum AgeRangerEnum {

    AGE_18("1", "18岁以下", 1,17),
    AGE_18_25("2", "18-25岁", 18,25),
    AGE_26_35("3", "26-35岁", 26,35),
    AGE_36_45("4", "36-45岁", 36,45),
    AGE_45("5", "45岁以上",46, 100);

    private String code;
    private String desc;
    private int begin;
    private int end;

    private AgeRangerEnum(String code, String desc, int begin, int end) {
        this.code = code;
        this.desc = desc;
        this.begin = begin;
        this.end = end;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public int getBegin() {
        return begin;
    }

    public int getEnd() {
        return end;
    }
}
