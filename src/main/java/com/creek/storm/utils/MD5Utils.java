package com.creek.storm.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Utils {

    public static String md5(String plainText) throws NoSuchAlgorithmException {

        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(plainText.getBytes());

        byte[] byteArray = md.digest();

        StringBuffer md5StrBuff = new StringBuffer();

        for (int i = 0; i < byteArray.length; i++) {
            if (Integer.toHexString(0xFF & byteArray[i]).length() == 1)
                md5StrBuff.append("0").append(Integer.toHexString(0xFF & byteArray[i]));
            else
                md5StrBuff.append(Integer.toHexString(0xFF & byteArray[i]));
        }

        return md5StrBuff.toString();
    }

    public static void main(String[] args) {

        String userID = "32327404226";//us1107648229
        try {
            System.out.println(md5(userID));//.substring(0, 4)
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
}
