package com.lym.test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class testSql {
    private static final String accessId = "LTAI4FjE9j7CiqEXAVAirG4w";
    private static final String accessKey = "4cC8Eq6Ssb4Ii7jEQM06cdXKv6ONE6";
    private static final String endPoint = "http://service.cn-hangzhou.maxcompute.aliyun-inc.com/api";
    private static final String project = "eric_20200213";
    private static final String sql = "select count(*) , getdate() FROM test07 where ds > -1 ;";

    public static void main(String[] args) {
        Account account = new AliyunAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.setEndpoint(endPoint);
        odps.setDefaultProject(project);
        Map<String,String> high = new HashMap<String, String>();
        high.put("odps.sql.submit.mode","");
        Instance i;
        try {
            i = SQLTask.run(odps, sql);
            i.waitForSuccess();
            List<Record> records = SQLTask.getResult(i);
            for (Record r : records) {
                System.out.println(r.get(0).toString());
            }
        } catch (OdpsException e) {
            e.printStackTrace();
        }
    }
}
