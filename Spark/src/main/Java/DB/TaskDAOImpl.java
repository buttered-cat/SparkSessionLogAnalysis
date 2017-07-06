package DB;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
import org.json.*;

/**
 * Created by Administrator on 2017/7/5.
 */
public class TaskDAOImpl implements  ITaskDAO {
    public MyTask findById(final int taskid) {
        final MyTask task = new MyTask();
        String sql = "select * from tasks where task_id=?";
        Object[] params = new Object[]{taskid};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {

            public void process(ResultSet rs) throws Exception {
                if(rs.next()) {
                    int taskID = rs.getInt(1);

                    task.taskID = taskID;

                    String taskParam = rs.getString(2);
                    Date startTime = rs.getDate(3);

                    task.taskStartTime = startTime;

                    JSONObject obj = new JSONObject(taskParam);

                    if(obj.has("ageLo"))
                        task.ageLo = obj.getInt("ageLo");

                    if(obj.has("ageHi"))
                        task.ageHi = obj.getInt("ageHi");

                    if(obj.has("professions"))
                    {
                        ArrayList<String> professions = new ArrayList<String>();
                        JSONArray professionsJson = obj.getJSONArray("professions");
                        if(professionsJson != null)
                        {
                            int len = professionsJson.length();
                            for(int i = 0; i < len; ++i)
                                professions.add(professionsJson.get(i).toString());
                        }
                        task.professions = professions;
                    }

                    if(obj.has("cities"))
                    {
                        ArrayList<String> cities = new ArrayList<String>();
                        JSONArray citiesJson = obj.getJSONArray("cities");
                        if(citiesJson != null)
                        {
                            int len = citiesJson.length();
                            for(int i = 0; i < len; ++i)
                                cities.add(citiesJson.get(i).toString());
                        }
                        task.cities = cities;
                    }

                    if(obj.has("keywords"))
                    {
                        ArrayList<String> keywords = new ArrayList<String>();
                        JSONArray keywordsJSON = obj.getJSONArray("keywords");
                        if(keywordsJSON != null)
                        {
                            int len = keywordsJSON.length();
                            for(int i = 0; i < len; ++i)
                                keywords.add(keywordsJSON.get(i).toString());
                        }
                        task.keywords = keywords;
                    }

                    if(obj.has("category_ids"))
                    {
                        ArrayList<Integer> categoryIDs = new ArrayList<Integer>();
                        JSONArray categoryIDJson = obj.getJSONArray("category_ids");
                        if(categoryIDJson != null)
                        {
                            int len = categoryIDJson.length();
                            for(int i = 0; i < len; ++i)
                                categoryIDs.add(categoryIDJson.getInt(i));
                        }
                        task.categoryIDs = categoryIDs;
                    }

                    if(obj.has("startFrom"))
                    {
                        SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        task.sessionStartFrom = datetimeFormat.parse(obj.getString("startFrom"));

                    }

                    if(obj.has("until"))
                    {
                        SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        task.sessionUntil = datetimeFormat.parse(obj.getString("until"));

                    }

                }
            }
        });
        return task;
    }
}
