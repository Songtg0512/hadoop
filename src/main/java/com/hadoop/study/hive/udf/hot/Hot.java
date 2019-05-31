/**
 * Copyright (C), 2015-2019, 学习
 * FileName: Hot
 * Author:   stg05
 * Date:     2019/5/29 16:20
 * Description: 计算热度
 * History:
 */
package com.hadoop.study.hive.udf.hot;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 〈计算热度〉
 *
 * @author stg05
 * @create 2019/5/29
 * @since 1.0.0
 */
public class Hot extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if(objectInspectors.length != 2){
            throw new UDFArgumentLengthException("args length error");
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        int a = Integer.valueOf(deferredObjects[0].get().toString());
        int b = Integer.valueOf(deferredObjects[1].get().toString());
        return a + b;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "Usage : Hot(int a,int b)";
    }
}
 
