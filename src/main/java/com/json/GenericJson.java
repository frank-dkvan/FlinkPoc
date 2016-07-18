package com.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Created by Gennady.Gilin on 6/19/2016.
 */
public abstract class GenericJson
{
    public String toString()
    {
        return new Gson().toJson( this );
    }

    public static <T> T fromString( String jsonString, Class<T> clazz ) {

        return ( new GsonBuilder()).create().fromJson( jsonString, clazz );
    }

}