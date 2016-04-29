package com.alibaba.datax.plugin.writer.hdfswriter;

/**
 * Created by shf on 15/10/8.
 * 
 * Modifed by zxlnet on 2016-04-29
 * added a new key pathSuffix
 */
public class Key {
    // must have
    public static final String PATH = "path";
    //must have
    public final static String DEFAULT_FS = "defaultFS";
    //must have
    public final static String FILE_TYPE = "fileType";
    // must have
    public static final String FILE_NAME = "fileName";
    // must have for column
    public static final String COLUMN = "column";
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String DATE_FORMAT = "dateFormat";
    // must have
    public static final String WRITE_MODE = "writeMode";
    // must have
    public static final String FIELD_DELIMITER = "fieldDelimiter";
    // not must, default UTF-8
    public static final String ENCODING = "encoding";
    // not must, default no compress
    public static final String COMPRESS = "compress";
    // not must, not default \N
    public static final String NULL_FORMAT = "nullFormat";

    //optional
    public static final String PATH_SUFFIX="pathSuffix";
}
