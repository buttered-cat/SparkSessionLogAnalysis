package DB;

/**
 * Created by Administrator on 2017/7/5.
 */
public class DAOFactory {
    /**
     * 获取任务管理DAO
     */
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }
}
