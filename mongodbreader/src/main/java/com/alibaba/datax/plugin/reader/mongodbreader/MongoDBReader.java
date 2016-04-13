package com.alibaba.datax.plugin.reader.mongodbreader;
import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.util.MongoUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.mongodb.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by zxlnet on 2016/04/12
 * 1. Support queryKey,queryKeyValueFormat,queryInterval
 * 2. Fill empty string to the field which does exists in mongodb collection
 */
public class MongoDBReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            return CollectionSplitUtil.doSplit(originalConfig,adviceNumber,mongoClient);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
                this.userName = originalConfig.getString(KeyConstant.MONGO_USER_NAME);
                this.password = originalConfig.getString(KeyConstant.MONGO_USER_PASSWORD);
                String database =  originalConfig.getString(KeyConstant.MONGO_DB_NAME);
            if(!Strings.isNullOrEmpty(this.userName) && !Strings.isNullOrEmpty(this.password)) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(originalConfig,userName,password,database);
            } else {
                this.mongoClient = MongoUtil.initMongoClient(originalConfig);
            }
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        private String database = null;
        private String collection = null;

        private JSONArray mongodbColumnMeta = null;
        private Long batchSize = null;
        /**
         * 用来控制每个task取值的offset
         */
        private Long skipCount = null;
        /**
         * 每页数据的大小
         */
        private int pageSize = 1000;

        @SuppressWarnings({ "deprecation", "unused", "rawtypes" })
		@Override
        public void startRead(RecordSender recordSender) {

            if(batchSize == null ||
                             mongoClient == null || database == null ||
                             collection == null  || mongodbColumnMeta == null) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                        MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }
            DB db = mongoClient.getDB(database);
            DBCollection col = db.getCollection(this.collection);
            DBObject obj = new BasicDBObject();
            obj.put(KeyConstant.MONGO_PRIMIARY_ID_META,1);

            long pageCount = batchSize / pageSize;
            long modCount = batchSize % pageSize;

            for(int i = 0; i <= pageCount; i++) {
                skipCount += i * pageSize;
                if(modCount == 0 && i == pageCount) {
                    break;
                }
                if (i == pageCount) {
                        pageCount = modCount;
                }
                
                Date now = new Date();
                BasicDBObject queryObj = new BasicDBObject();  
                
                String queryInterval = readerSliceConfig.getString(KeyConstant.QUERY_INTERVAL).toLowerCase();
                
                if (queryInterval.equals("hourly"))
                {
	                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH");
	                String startTime=df.format(new Date(now.getTime() - 60*60*1000)) +":00:00";
	                String endTime=df.format(now) +":00:00";
	                
	                System.out.println("Date Range >>> " + startTime + " - " + endTime );
	                
	                queryObj.put(readerSliceConfig.getString(KeyConstant.QUERY_KEY), new BasicDBObject("$gte", startTime).append("$lt", endTime));
	            }
                else if (queryInterval.equals("daily"))
                {
                	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                	SimpleDateFormat dfFull = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                	SimpleDateFormat targetdf = new SimpleDateFormat(readerSliceConfig.getString(KeyConstant.QUERY_KEY_VALUE_FORMAT));
                	
                	
					Date startTime;
					String sStartTime="";
					try {
						startTime = dfFull.parse(df.format(new Date(now.getTime() - 24*60*60*1000)) +" 00:00:00");
						sStartTime = targetdf.format(startTime);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	                
	                
					Date endTime;
					String sEndTime="";
					try {
						endTime = dfFull.parse(df.format(now) +" 00:00:00");
						sEndTime = targetdf.format(endTime);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	                
					System.out.println("Date Range >>> " + sStartTime + " - " + sEndTime );
					
	                queryObj.put(readerSliceConfig.getString(KeyConstant.QUERY_KEY), new BasicDBObject("$gte", sStartTime).append("$lt", sEndTime));
                }
                
                DBCursor dbCursor = col.find(queryObj).sort(obj).skip(skipCount.intValue()).limit(pageSize);
                
                while (dbCursor.hasNext()) {
                    DBObject item = dbCursor.next();
                    Record record = recordSender.createRecord();
                    Iterator columnItera = mongodbColumnMeta.iterator();
                    while (columnItera.hasNext()) {
                    	JSONObject column = (JSONObject)columnItera.next();
                        Object tempCol = item.get(column.getString(KeyConstant.COLUMN_NAME));
                    	
                        if (tempCol == null) {
                        	//fill empty string if the field does not exists in mongodb
                        	record.addColumn(new StringColumn(""));
                        	continue;
                        }
                        
                        if (tempCol instanceof Double) {
                            record.addColumn(new DoubleColumn((Double) tempCol));
                        } else if (tempCol instanceof Boolean) {
                            record.addColumn(new BoolColumn((Boolean) tempCol));
                        } else if (tempCol instanceof Date) {
                            record.addColumn(new DateColumn((Date) tempCol));
                        } else if (tempCol instanceof Integer) {
                            record.addColumn(new LongColumn((Integer) tempCol));
                        }else if (tempCol instanceof Long) {
                            record.addColumn(new LongColumn((Long) tempCol));
                        } else {
                            if(KeyConstant.isArrayType(column.getString(KeyConstant.COLUMN_TYPE))) {
                                String splitter = column.getString(KeyConstant.COLUMN_SPLITTER);
                                if(Strings.isNullOrEmpty(splitter)) {
                                    throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                                            MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
                                } else {
                                    ArrayList array = (ArrayList)tempCol;
                                    String tempArrayStr = Joiner.on(splitter).join(array);
                                    record.addColumn(new StringColumn(tempArrayStr));
                                }
                            } else {
                                record.addColumn(new StringColumn(tempCol.toString()));
                            }
                        }
                    }
                    recordSender.sendToWriter(record);
                }
            }
        }

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
                this.userName = readerSliceConfig.getString(KeyConstant.MONGO_USER_NAME);
                this.password = readerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD);
                this.database = readerSliceConfig.getString(KeyConstant.MONGO_DB_NAME);
            if(!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
                mongoClient = MongoUtil.initCredentialMongoClient(readerSliceConfig,userName,password,database);
            } else {
                mongoClient = MongoUtil.initMongoClient(readerSliceConfig);
            }
            
            this.collection = readerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
            this.mongodbColumnMeta = JSON.parseArray(readerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
            this.batchSize = readerSliceConfig.getLong(KeyConstant.BATCH_SIZE);
            this.skipCount = readerSliceConfig.getLong(KeyConstant.SKIP_COUNT);
        }

        @Override
        public void destroy() {

        }
    }
}
