package com.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Created by Gennady.Gilin on 6/23/2016.
 */
public class TimeUtils {

    private static DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss:SSS");

    public static String timestampToString( long timeStamp ){

        Instant fromUnixTimestamp = Instant.ofEpochSecond( timeStamp );
        LocalDateTime localDateTime = LocalDateTime.ofInstant( fromUnixTimestamp, ZoneId.systemDefault() );
        String timeStampString = localDateTime.format( format );
        //String timeStampString = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format( new Date( timeStamp ));
        return timeStampString;
    }

    public static String currentDataToString(){

        String timeStampString = LocalDateTime.now().format( format );
        //String timeStampString = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format( new Date( timeStamp ));
        return timeStampString;
    }
}
