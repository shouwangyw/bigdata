package com.yw.recommend.program;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;

public class IOUtils {

    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader("D:\\点播数据\\item.csv"));

        String line = reader.readLine();
        ArrayList<String> ids = new ArrayList<>();

        while (line != null) {
            ids.add(line.split(",")[0]);
            line = reader.readLine();
        }

        BufferedReader reader2 = new BufferedReader(new FileReader("D:\\点播数据\\点播数据\\20190803.csv"));
        BufferedWriter bw = new BufferedWriter(new FileWriter("D:\\点播数据\\20190803_tmp.csv"));
        String line2 = reader2.readLine();
        while (line2 != null) {
            System.out.println(line2);
            StringBuilder builder = new StringBuilder();
            String[] elems = line2.split(",");
            for (int i = 0; i < elems.length; i++) {
                if (i == 1) {
                    builder.append(",").append(ids.get(new Random().nextInt(ids.size())));
                } else if (i != 2 && i != 6) {
                    builder.append(",").append(elems[i]);
                }
            }
            bw.write(builder.substring(1) + "\n");
            line2 = reader2.readLine();
        }
        bw.flush();
        bw.close();
    }
}
