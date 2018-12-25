import org.apache.spark.sql.Row;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.Serializable;

/*
* 这个抽象类是为了防止collect的相关操作，数据全部到driver端，然后jvm内存溢出，直接通过foreachpartition
* 的操作让数据直接在exucutor上面做操作。
*
* */

public abstract class JavaForeachPartitionFunc extends AbstractFunction1<Iterator<Row>, BoxedUnit> implements Serializable {
    @Override
    public BoxedUnit apply(Iterator<Row> it) {
        call(it);
        return BoxedUnit.UNIT;
    }

    public abstract void call(Iterator<Row> it);
}