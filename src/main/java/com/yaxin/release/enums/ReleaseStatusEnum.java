package com.yaxin.release.enums;

import java.util.Arrays;
import java.util.List;

/**
 * 投放流程环节
 */
public enum ReleaseStatusEnum {

    NOTCUSTOMER("00", "非目标客户"),
    CUSTOMER("01", "目标客户"),
    BIDING("02", "竞价"),
    SHOW("03", "曝光"),
    CLICK("04", "点击"),
    ARRIVE("05", "到达"),
    REGISTER("06", "注册(用户)");

    private String code;
    private String desc;

    private ReleaseStatusEnum(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static List<String> getReleaseStatus(){
        List<String> status = Arrays.asList(
                NOTCUSTOMER.code,
                CUSTOMER.code,
                BIDING.code,
                SHOW.code,
                CLICK.code,
                ARRIVE.code,
                REGISTER.code
        );
        return status;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

}
