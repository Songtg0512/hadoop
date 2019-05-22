/**
 * Copyright (C), 2015-2019, 学习
 * FileName: GendorToNumber
 * Author:   stg05
 * Date:     2019/5/22 17:14
 * Description: 性别转数组
 * History:
 */
package com.hadoop.study.hive.udf.genderToNum;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 〈性别转数字〉
 *
 * @author stg05
 * @create 2019/5/22
 * @since 1.0.0
 */
// com.hadoop.study.hive.udf.genderToNum.GendorToNumber
public class GendorToNumber extends GenericUDF {

    private ObjectInspectorConverters.Converter[] converters;

    // select myFunction(gender)  对参数数据进行转化
    @Override
    public ObjectInspector initialize(ObjectInspector[] agruments) throws UDFArgumentException {
        if(agruments.length != 1){
            throw new UDFArgumentException("agruments is not right");
        }

        // 通过此参数，转化接收传递过来的值，后面使用
        converters = new ObjectInspectorConverters.Converter[agruments.length];
        for(int i = 0; i< agruments.length; i++){
            converters[i] = ObjectInspectorConverters.getConverter(agruments[i], PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }
        return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
    }

    // 使用转化好的参数值
    @Override
    public Object evaluate(DeferredObject[] agruments) throws HiveException {
        if(agruments[0].get() == null){
            return null;
        }
        String genderName = (String) converters[0].convert(agruments[0].get());
        return stringToNum(genderName);
    }

    public String stringToNum(String gender){
        if(gender.equals("male")){
            return "1";
        }else{
            return "2";
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        // 返回函数内容
        return "this function is used to change gender to number";
    }
}
 
