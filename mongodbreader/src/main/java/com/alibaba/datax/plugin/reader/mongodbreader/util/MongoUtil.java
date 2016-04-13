package com.alibaba.datax.plugin.reader.mongodbreader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReaderErrorCode;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jianying.wcj on 2015/3/17 0017.
 * Modified by zxlnet on 2016/04/12
 * 1. Support connect MongoDB through MongoDBURI
 * 2. default ReadPreference => secondaryPreferred
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

    public static void main(String[] args) {
        try {
        	
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
