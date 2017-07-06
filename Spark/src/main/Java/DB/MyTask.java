package DB;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

/**
 * Created by Administrator on 2017/7/5.
 */
public class MyTask implements Serializable{
    public int taskID;
    public int ageLo;
    public int ageHi;

    public ArrayList<String> professions;
    public ArrayList<String> cities;
    public ArrayList<String> keywords;
    public ArrayList<Integer> categoryIDs;

    public Date sessionStartFrom;
    public Date sessionUntil;
    public Date taskStartTime;

    public MyTask()
    {
        taskID = -1;
        ageLo = -1;
        ageHi = -1;

        professions = null;
        cities = null;
        keywords = null;
        categoryIDs = null;
        sessionStartFrom = null;
        sessionUntil = null;
        taskStartTime = null;
    }

    private static final long serialVersionUID = -1466479389299512377L;

    public static long getSerialversionuid() {
        return serialVersionUID;
    }
}
