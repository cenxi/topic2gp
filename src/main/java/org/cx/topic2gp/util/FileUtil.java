package org.cx.topic2gp.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Created by 冯曦 on 2017/12/23.
 */
public class FileUtil {

    public static void zipFiles(List<File> files, String destFileStr) throws IOException {
        if (files.size() == 0) {
            return;
        }
        File zipFile = new File(destFileStr);
        ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(zipFile));
        for (File file : files) {
            FileInputStream fis = new FileInputStream(file);
            zipOut.putNextEntry(new ZipEntry(file.getName()));
            byte[] bytes = new byte[2048];
            int temp = 0;
            while(-1 != (temp = fis.read(bytes))){
                zipOut.write(bytes,0,temp);
                zipOut.flush();
            }
            fis.close();
        }
        zipOut.close();
    }

    public static void deleteFile(List<File> files) {
        for (File file : files) {
            file.delete();
        }
    }
}
