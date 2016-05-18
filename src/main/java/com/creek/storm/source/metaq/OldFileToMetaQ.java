package com.creek.storm.source.metaq;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import com.alibaba.da.plough.output.MetaQOutput;

public class OldFileToMetaQ {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        MetaQOutput output = new MetaQOutput("ae_beacon_pageview", "test", "test");

        FileReader in = new FileReader("d:\\1.txt");
        @SuppressWarnings("resource")
        BufferedReader ins1 = new BufferedReader(in);
        //		output.output("11", msg)
        String s = null;
        while ((s = ins1.readLine()) != null) {
            String a = s.trim();
            String b = a.replace("\001", "\005").replace("\\N", "") + "\005" + "aaa" + "\005"
                    + "bbb" + "\005" + "ccc" + "\005" + "ddd";
            String c[] = b.split("\005");
            System.out.println(c.length);
            System.out.println(b);
            output.output("11", b);
        }
        // output.output("1", sb.toString());
        output.close();
        // MetaPushConsumer consumer = new MetaPushConsumer("sekpi3");
        // consumer.subscribe(topic, subExpression)
    }

}
