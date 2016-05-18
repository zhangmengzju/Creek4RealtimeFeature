package com.alibaba.dt.guider.storm.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DateUtils {

    public static long DateToLong(String format, String dateStr, TimeZone tZone)
            throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(tZone);

        Date dt = sdf.parse(dateStr);

        return dt.getTime();
    }

    @SuppressWarnings("deprecation")
    public static String conventTimeToLocaleString(long time, String timeZone) {
        // 中国时间    timeZone="Asia/Shanghai"   
        // 美国时间    timeZone="America/Los_Angeles"
        TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
        return new Date(time).toLocaleString();
    }

    public static String conventTimeToString(long time, String timeZone) {
        // 中国时间    timeZone="Asia/Shanghai"   
        // 美国时间    timeZone="America/Los_Angeles"
        TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
        return new Date(time).toString();
    }

    public static int compareDate() {
        String inputDate1 = "2011-05-14 23:30:00";
        String inputDate2 = "2011-05-14 23:35:00";
        Date date1 = null;
        Date date2 = null;

        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            date1 = inputFormat.parse(inputDate1);
            date2 = inputFormat.parse(inputDate2);
        } catch (ParseException e) {
        }

        return date1.compareTo(date2);
    }

    //    public static void converter() {
    //        String inputDate = "2011-05-14 23:30:00";
    //        TimeZone timeZoneSH = TimeZone.getTimeZone("Asia/Shanghai");
    //        TimeZone timeZoneNY = TimeZone.getTimeZone("America/New_York");
    //        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //        inputFormat.setTimeZone(timeZoneSH);
    //        Date date = null;
    //        try {
    //            date = inputFormat.parse(inputDate);
    //        } catch (ParseException e) {
    //        }
    //
    //        SimpleDateFormat outputFormat = new SimpleDateFormat("EEE MMM d HH:mm:ss Z yyyy", Locale.US);
    //        outputFormat.setTimeZone(timeZoneSH);
    //        //System.out.println("Asia/Shanghai:" + outputFormat.format(date));
    //        outputFormat.setTimeZone(timeZoneNY);
    //        //System.out.println("America/New_York:" + outputFormat.format(date));
    //    }

    public static void main(String[] args) throws ParseException {

        //        TimeZone timeZoneSH = TimeZone.getTimeZone("Asia/Shanghai");
        //        TimeZone timeZoneLA = TimeZone.getTimeZone("America/Los_Angeles");
        //
        //        String format = "yyyy-MM-dd HH:mm:ss";
        //        String dateStr = "2015-08-26 01:38:46";
        //
        //        long timeLongSH = DateToLong(format, dateStr, timeZoneSH);
        //        System.out.println("[Long timeLongSH]" + timeLongSH);
        //
        //        long timeLongLA = DateToLong(format, dateStr, timeZoneLA);
        //        System.out.println("[Long timeLongSH]" + timeLongLA);

        //        String timeLocaleString = conventTimeToLocaleString(timeLong, "Asia/Shanghai");
        //        System.out.println("[LocaleString]" + timeLocaleString);
        //
                Long timeLong = 1448351460000L;
                String timeString = conventTimeToString(timeLong, "America/Los_Angeles");
                System.out.println("[String]" + timeString);

        //        converter();
        //        compareDate();

        //        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles"));
        //        Date currentDate = calendar.getTime();
        //        System.out.println("[curTime Long]" + currentDate);
        //
        //        Calendar calendar1 = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
        //        Date currentDate1 = calendar1.getTime();
        //        System.out.println("[curTime Long]" + currentDate1);      

        Date date = new Date();
        System.out.println("[date]" + date.toString());
        //
        //        Long curTime = DateUtils.DateToLong("EEE MMM dd hh:mm:ss z yyyy", date.toString(),
        //                TimeZone.getTimeZone("America/Los_Angeles"));
        //        System.out.println("[curTime]" + curTime);

    }

}
