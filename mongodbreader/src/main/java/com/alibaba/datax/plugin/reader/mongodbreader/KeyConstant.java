package com.alibaba.datax.plugin.reader.mongodbreader;

/**
 * Created by jianying.wcj on 2015/3/17 0017.
 * Modified by zxlnet 2016/4/12, added queryKey,queryInterval,queryKeyValueFormat

 * Modified by zxlnet 2016/04/20
 * 增加JSON，JSON Array,Dummy,Constant几种column type。column增加value，目前只对constant类型栏位生效
*/

public class KeyConstant {
    /**
     * 数组类型
     */
    public static final String ARRAY_TYPE = "array";
    /**
     * mongodb 的 host 地址
     */
    public static final String MONGO_ADDRESS = "address";
    /**
     * mongodb 的用户名
     */
    public static final String MONGO_USER_NAME = "userName";
    /**
     * mongodb 密码
     */
    public static final String MONGO_USER_PASSWORD = "userPassword";
    /**
     * mongodb 数据库名
     */
    public static final String MONGO_DB_NAME = "dbName";
    /**
     * mongodb 集合名
     */
    public static final String MONGO_COLLECTION_NAME = "collectionName";
    /**
     * mongodb 的列
     */
    public static final String MONGO_COLUMN = "column";
    /**
     * 每个列的名字
     */
    public static final String COLUMN_NAME = "name";
    /**
     * 每个列的类型
     */
    public static final String COLUMN_TYPE = "type";
    /**
     * 列分隔符
     */
    public static final String COLUMN_SPLITTER = "splitter";
    /**
     * 跳过的列数
     */
    public static final String SKIP_COUNT = "skipCount";
    /**
     * 批量获取的记录数
     */
    public static final String BATCH_SIZE = "batchSize";
    /**
     * MongoDB的idmeta
     */
    public static final String MONGO_PRIMIARY_ID_META = "_id";
    /**
     * 判断是否为数组类型
     * @param type 数据类型
     * @return
     */
    public static boolean isArrayType(String type) {
        return ARRAY_TYPE.equals(type);
    }
    /**
     * Mongo连接方式
     */
    public static final String MONGO_CONNECT_TYPE = "connectType";
    /**
     * 数据过滤关键字，目前只支持单关键字
     */
    public static final String QUERY_KEY = "queryKey";
    /**
     * 过滤值的格式
     */
    public static final String QUERY_KEY_VALUE_FORMAT = "queryKeyValueFormat";
    /**
     * 抓去数据的间隔
     */
    public static final String QUERY_INTERVAL = "queryInterval";
    
    /**
     * JSON类型
     */
    public static final String JSON_TYPE = "json";
    /**
     * 判断是否为JSON类型
     * @param type 数据类型
     * @return
     */
    public static boolean isJsonType(String type) {
        return JSON_TYPE.equals(type);
    }
    /**
     * JSON Array类型
     */
    public static final String JSONARRAY_TYPE = "jsonarray";
    /**
     * 判断是否为JSON Array类型
     * @param type 数据类型
     * @return
     */
    public static boolean isJsonArrayType(String type) {
        return JSONARRAY_TYPE.equals(type);
    }
    /**
     * Dummy类型
     */
    public static final String DUMMY_TYPE = "dummy";
    /**
     * 判断是否为Dummy类型
     * @param type 数据类型
     * @return
     */
    public static boolean isDummyType(String type) {
        return DUMMY_TYPE.equals(type);
    }
    /**
     * Constant类型
     */
    public static final String CONSTANT_TYPE = "constant";
    /**
     * 判断是否为Constant类型
     * @param type 数据类型
     * @return
     */
    public static boolean isConstantType(String type) {
        return CONSTANT_TYPE.equals(type);
    }
    /**
     * 每个列的默认值
     */
    public static final String COLUMN_VALUE = "value";
    /**
     * now类型
     */
    public static final String VALUE_NOW = "now";
    /**
     * 判断是否为Now类型
     * @param type 数据类型
     * @return
     */
    public static boolean isValueNow(String type) {
        return VALUE_NOW.equals(type);
    }
    /**
     * mongoDB查询index hint
     */
    public static final String INDEX_HINT = "indexHint";
    
    /**
     * 数据抓去起始时间，仅在queryInterval=range时有效
     */
    public static final String START_TIME = "startTime";
    /**
     * 数据抓去结束时间，仅在queryInterval=range时有效
     */
    public static final String END_TIME = "endTime";
}
