package org.example;

import java.io.IOException;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        String path = "db.db";

        try {
            DAL dal = new DAL(path);

            Page p = dal.allocateEmptyPage();
            byte[] dataToCopy = "data".getBytes();
            System.arraycopy(dataToCopy, 0, p.getData(), 0, dataToCopy.length);
            dal.writePage(p);

            dal.close();

            dal = new DAL(path);

            p = dal.allocateEmptyPage();
            byte[] dataToCopy2 = "data2".getBytes();
            System.arraycopy(dataToCopy2, 0, p.getData(), 0, dataToCopy2.length);
            dal.writePage(p);

            p = dal.allocateEmptyPage();
            dal.releasePage(p);

            dal.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }
}
