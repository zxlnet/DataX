package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by jianying.wcj on 2015/3/17 0017.
 * Modified by zxlnet on 2016/04/12
 * 1. Support connect MongoDB through MongoDBURI
 * 2. default ReadPreference => secondaryPreferred
 * 3. add a new method: getQueries
 */
public class MongoUtil {

    public static MongoClient initMongoClient(Configuration conf) {
    	if (conf.getString(KeyConstant.MONGO_ADDRESS).toLowerCase().equals("mongoclient")){
    		return getMongoClient(conf);
    	}
    	else
    	{
    		return getMongoClientViaURI(conf);
    	}
    }
    
    private static MongoClient getMongoClientViaURI(Configuration conf) {
        try {
        	System.out.println(conf.getString(KeyConstant.MONGO_ADDRESS));
        	MongoClientURI uri = new MongoClientURI(conf.getString(KeyConstant.MONGO_ADDRESS));
        	
        	MongoClient client = new MongoClient(uri);
        	
        	return client;
        } catch (NumberFormatException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,"不合法参数");
        } catch (Exception e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.UNEXCEPT_EXCEPTION,"未知异常");
        }   
    }

    public static MongoClient getMongoClient(Configuration conf) {
    	List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
        if(addressList == null || addressList.size() <= 0) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,"不合法参数");
        }
        try {
        	MongoClient client = new MongoClient(parseServerAddress(addressList));
        	client.setReadPreference(ReadPreference.secondaryPreferred());
        	return client;
        } catch (UnknownHostException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_ADDRESS,"不合法的地址");
        } catch (NumberFormatException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,"不合法参数");
        } catch (Exception e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.UNEXCEPT_EXCEPTION,"未知异常");
        }
    }
    
    public static MongoClient initCredentialMongoClient(Configuration conf,String userName,String password,String database) {
        List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
        if(!isHostPortPattern(addressList)) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,"不合法参数");
        }
        try {
            MongoCredential credential = MongoCredential.createCredential(userName, database, password.toCharArray());
            return new MongoClient(parseServerAddress(addressList), Arrays.asList(credential));

        } catch (UnknownHostException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_ADDRESS,"不合法的地址");
        } catch (NumberFormatException e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,"不合法参数");
        } catch (Exception e) {
            throw DataXException.asDataXException(MongoDBReaderErrorCode.UNEXCEPT_EXCEPTION,"未知异常");
        }
    }
    
    /**
     * 判断地址类型是否符合要求
     * @param addressList
     * @return
     */
    private static boolean isHostPortPattern(List<Object> addressList) {
        boolean isMatch = false;
        for(Object address : addressList) {
            String regex = "([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+):([0-9]+)";
            if(((String)address).matches(regex)) {
                isMatch = true;
            }
        }
        return isMatch;
    }
    /**
     * 转换为mongo地址协议
     * @param rawAddressList
     * @return
     */
    private static List<ServerAddress> parseServerAddress(List<Object> rawAddressList) throws UnknownHostException{
        List<ServerAddress> addressList = new ArrayList<ServerAddress>();
        for(Object address : rawAddressList) {
            String[] tempAddress = ((String)address).split(":");
            try {
                ServerAddress sa = new ServerAddress(tempAddress[0],Integer.valueOf(tempAddress[1]));
                addressList.add(sa);
            } catch (Exception e) {
                throw new UnknownHostException();
            }
        }
        return addressList;
    }
    
    /*根据配置生成查询条件*/
    /*added by zxlnet on 2016-04-14*/
    public static BasicDBObject getQueries(Configuration conf){
    	
    	BasicDBObject queryObj = new BasicDBObject(); 
        
        String queryInterval = conf.getString(KeyConstant.QUERY_INTERVAL).toLowerCase();
        
        Date now = new Date();
        if (queryInterval.equals("hourly"))
        {
        	/*同步前一小时数据*/
            String startTime = getHourStart(new Date(now.getTime() - 60*60*1000));
            String endTime = getHourStart(now);
            
            //System.out.println("Date range:" + startTime + " "  +  endTime);
            
            queryObj.put(conf.getString(KeyConstant.QUERY_KEY), new BasicDBObject("$gte", startTime).append("$lt", endTime));
            return queryObj;
        }
        else if (queryInterval.equals("daily"))
        {
        	/*同步前一天的数据*/
			String startTime=formatDate(conf.getString(KeyConstant.QUERY_KEY_VALUE_FORMAT),getDateStart(new Date(now.getTime() - 24*60*60*1000)));
			String endTime=formatDate(conf.getString(KeyConstant.QUERY_KEY_VALUE_FORMAT),getDateStart(now));
			
            queryObj.put(conf.getString(KeyConstant.QUERY_KEY), new BasicDBObject("$gte", startTime).append("$lt", endTime));
            return queryObj;
        } 
        else if (queryInterval.equals("range"))
        {
        	String startTime = conf.getString(KeyConstant.START_TIME);
        	String endTime = conf.getString(KeyConstant.END_TIME);
            queryObj.put(conf.getString(KeyConstant.QUERY_KEY), new BasicDBObject("$gte", startTime).append("$lt", endTime));
            
            return queryObj;
        }
        
        queryObj.put(conf.getString(KeyConstant.QUERY_KEY), new BasicDBObject("$gte", "2011-01-01 00:00:00").append("$lt", "2011-01-01 00:00:00"));
        
        return queryObj;
    }
    
    
    private static String getHourStart(Date d){
        return new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(d) ;
    }
    
    private static String getDateStart(Date d){
        return new SimpleDateFormat("yyyy-MM-dd 00:00:00").format(d) ;
    }
    
    private static String formatDate(String targetFormat, String d){
    	SimpleDateFormat targetdf = new SimpleDateFormat(targetFormat);
    	
    	String r="";
    	
    	try {
    		r = targetdf.format(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(d));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return r;
    }
    
    /*根据配置生成查询的字段*/
    /*added by zxlnet on 2016-04-14*/
    public static BasicDBObject getFields(Configuration conf,JSONArray mongodbColumnMeta){
    	
    	BasicDBObject fieldObj = new BasicDBObject(); 
    	
    	@SuppressWarnings("rawtypes")
		Iterator columnItera = mongodbColumnMeta.iterator();
        
        while (columnItera.hasNext()) {
        	JSONObject column = (JSONObject)columnItera.next();
            
            	if (!KeyConstant.isDummyType(column.getString(KeyConstant.COLUMN_TYPE)) &&
            			!KeyConstant.isConstantType(column.getString(KeyConstant.COLUMN_TYPE)) &
            			!KeyConstant.isArrayType(column.getString(KeyConstant.COLUMN_TYPE)))
            	{
            		fieldObj.put(column.getString(KeyConstant.COLUMN_NAME), 1);
            	}
            }
        
        return fieldObj;
    }
    
    public static void main(String[] args) {
        try {
        	JSONArray jsons = JSON.parseArray("[ { \"packageName\" : \"com.sohu.inputmethod.sogou\" , \"launchCount\" : 0.0 , \"usageTime\" : 850809.0} , { \"packageName\" : \"com.android.documentsui\" , \"launchCount\" : 0.0 , \"usageTime\" : 3664.0} ]");
        	
        	for(int idx=0;idx<jsons.size();idx++){
            	HashMap jsonMap = JSON.parseObject(jsons.get(idx).toString(), HashMap.class);
            	Iterator iter = jsonMap.entrySet().iterator();
            	while (iter.hasNext()) {
        	       Map.Entry entry = (Map.Entry) iter.next();
        	       Object key = entry.getKey();
        	       Object val = entry.getValue();
        	       
        	       System.out.println(key + "-" + val);
            	}
        	}
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
