package DB;

/**
 * Created by Administrator on 2017/7/5.
 */
public interface ITaskDAO {
    /**
     * 根据主键查询业务
     */
    MyTask findById(int taskid);
}
