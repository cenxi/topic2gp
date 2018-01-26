package org.cx.topic2gp.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by 冯曦 on 2017/12/20.
 */
public class RemoteFileUtil {

    /**
     * Copy files from server A to server B's dir
     * @param aip
     * @param ausername
     * @param apwd
     * @param apath
     * @param bip
     * @param busername
     * @param bpwd
     * @param bpath
     * @return      没有拷贝成功的文件列表
     */
    public static List<String> CopyFileA2C(String aip,String ausername,String apwd,String apath,
                                   String bip,String busername,String bpwd,String bpath) {
        RemoteExecuteCommand acommand = new RemoteExecuteCommand(aip, ausername, apwd);
        RemoteExecuteCommand bcommand = new RemoteExecuteCommand(bip, busername, bpwd);
        List<String> files = acommand.executeReturn("ls " + apath);
        List<String> errorsFiles = new ArrayList<>();
        for (String file : files) {
            //A-->B
            boolean flag=acommand.copyRemoteFile(apath + "/" + file, "/tmp/");
            if(!flag){
                errorsFiles.add(file);
                continue;
            }
            //B-->C
            flag=bcommand.putRemoteFile("/tmp/" + file, bpath);
            if(!flag){
                errorsFiles.add(file);
            }
        }
        return errorsFiles;
    }

    public static void main(String[] args) {
        CopyFileA2C("192.168.8.161", "root", "runbaomi", "/root/tmp",
                "192.168.8.163", "root", "runbaomi", "/root/tmp");
    }
}
