package hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 自定义UDF函数
 *
 * 需要继承GenericUDF类，重写抽象方法。
 *
 * 函数的调用： select my_len('abcd');
 * UDF
 */
public class StringLengthUDF extends GenericUDF {
    /**
     * 初始化方法 判断传入到参数的个数，类型等，约定函数的返回值类型
     * @param objectInspectors 传入到函数的参数类型
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        //简单判断
        //判断参数的个数
        if(objectInspectors.length != 1){
            throw new UDFArgumentLengthException("Input Args Length Error!!!");
        }
        //判断类型
        if(!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentTypeException(0,"Input Args Type Error!!!");
        }
        //约定函数的返回值类型
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector; //int
    }

    /**
     * 函数的逻辑处理
     * @param deferredObjects 传入到函数的参数值
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        //获取参数
        Object o = deferredObjects[0].get();
        return o.toString().length();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }
}
