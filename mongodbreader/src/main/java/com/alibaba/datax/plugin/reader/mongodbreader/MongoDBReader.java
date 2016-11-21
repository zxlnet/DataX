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

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by zxlnet on 2016/04/12
 * 1. Support queryKey,queryKeyValueFormat,queryInterval
 * 2. Fill empty string to the field which does exists in mongodb collection
 * 
 * Modified by zxlnet on 2016/04/14
 * 修正Task分片读取代码的逻辑错误
 * 
 * Modified by zxlnet 2016/04/20
 * 增加JSON，JSON Array,Dummy,Constant几种column type。column增加value，目前只对constant类型栏位生效
 * JSON反序相应的field会加在主表
 * JSONARRAY反序后会逐条保存
 * Dummy用来填出字段数避免报错
 * Constant直接填充value值，一般用来放默认值
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
            
            System.out.println("batchSize=" + batchSize);
            System.out.println("pageCount=" + pageCount);
            System.out.println("modCount=" + modCount);
            System.out.println("skipCount=" + skipCount);
            System.out.println("pageSize=" + pageSize);
            
            for(int i = 0; i <= pageCount; i++) {
                if (i == pageCount) {
                        pageSize = new Long(modCount).intValue();
                }
                
                BasicDBObject queryObj = MongoUtil.getQueries(readerSliceConfig);
                
            	BasicDBObject fieldObj = MongoUtil.getFields(readerSliceConfig,mongodbColumnMeta);
                
                DBCursor dbCursor = null;

                String indexHint=readerSliceConfig.getString(KeyConstant.INDEX_HINT);
                
                if (indexHint!=null && !indexHint.equals("")){	
                	dbCursor = col.find(queryObj,fieldObj).hint(indexHint).skip(skipCount.intValue()).limit(pageSize);
                }
                else {
                	dbCursor = col.find(queryObj,fieldObj).skip(skipCount.intValue()).limit(pageSize);
                }
                
                while (dbCursor.hasNext()) {
                    DBObject item = dbCursor.next();
                    Record record = recordSender.createRecord();
                    Iterator columnItera = mongodbColumnMeta.iterator();
                    
                    while (columnItera.hasNext()) {
                    	JSONObject column = (JSONObject)columnItera.next();
                    	//System.out.println(item.get(column.getString(KeyConstant.COLUMN_NAME)));
                        Object tempCol = item.get(column.getString(KeyConstant.COLUMN_NAME));
                        if (tempCol == null) {
                        	if (KeyConstant.isDummyType(column.getString(KeyConstant.COLUMN_TYPE)) ||
                        			KeyConstant.isConstantType(column.getString(KeyConstant.COLUMN_TYPE)))
                        	{
                        		tempCol = new Object();//dummy column
                        	}
                        	else
                        	{
	                        	//fill empty string if the field does not exists in mongodb
	                        	record.addColumn(new StringColumn(""));
	                        	continue;
                        	}
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
                            } else if(KeyConstant.isJsonType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            	HashMap jsonMap = JSON.parseObject(tempCol.toString(), HashMap.class);
                            	Iterator iter = jsonMap.entrySet().iterator();
                            	while (iter.hasNext()) {
                        	       Map.Entry entry = (Map.Entry) iter.next();
                        	       Object key = entry.getKey();
                        	       Object val = entry.getValue();
                        		   record.addColumn(new StringColumn(val.toString()));
                            	}
                            } else if(KeyConstant.isJsonArrayType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            	JSONArray jsons = JSON.parseArray(tempCol.toString());
                            	
                            	//System.out.println(record.getColumnNumber());
                            	
                            	if (jsons.size()==0) {
                            		record=null;
                            		break;
                            	}
                            	
                            	for(int idx=0;idx<jsons.size();idx++){
                                    Record rec = cloneRecord(recordSender,record);
                                    HashMap jsonMap = JSON.parseObject(jsons.get(idx).toString(), HashMap.class);
                                	Iterator iter = jsonMap.entrySet().iterator();
                                	while (iter.hasNext()) {
                            	       Map.Entry entry = (Map.Entry) iter.next();
                            	       Object key = entry.getKey();
                            	       Object val = entry.getValue();
                            		   rec.addColumn(new StringColumn(val.toString()));
                                	}
                                	recordSender.sendToWriter(rec);
                            	}
                            	
                            	record = null;
                            	
                            } else if (KeyConstant.isDummyType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            	continue;
                            } else if (KeyConstant.isConstantType(column.getString(KeyConstant.COLUMN_TYPE))) {
                            	
                            	if (KeyConstant.isValueNow(column.getString(KeyConstant.COLUMN_VALUE))){
                            		record.addColumn(new StringColumn(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())));
                            	}
                            	else
                            	{
                            		record.addColumn(new StringColumn(column.getString(KeyConstant.COLUMN_VALUE)));
                            	}
                            } else {
                                record.addColumn(new StringColumn(tempCol.toString()));
                            }
                        }
                    }
                    
                    if (record!=null && record.getColumnNumber()==this.mongodbColumnMeta.size()){
                    	recordSender.sendToWriter(record);
                    }
                }
                
                skipCount += pageSize;

            }
        }
        
        private Record cloneRecord(RecordSender recordSender,Record src){
        	Record tgt = recordSender.createRecord();
        	
        	for(int i=0;i<src.getColumnNumber();i++){
        		tgt.addColumn(src.getColumn(i));
        	}
        	
        	return tgt;
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
