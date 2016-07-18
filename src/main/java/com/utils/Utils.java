package com.utils;

import java.util.List;

/**
 * Created by Gennady.Gilin on 6/23/2016.
 */
public class Utils {

    public static String ListOfIntToString( List<Integer> numbers ){

        String result = numbers.toString();
        result = result.replaceFirst( "\\[", "\",");
        result = result.replaceFirst( "\\]", ",\"");
        result = result.replaceAll(",", "\\\\,");

        return result;
    }
}
