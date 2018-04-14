using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;

namespace ConsoleApp_everyday_spider
{
    class Program
    {
        /*make use of  the API to get the video information of the current day, 
        cate_id=30 is the V·U zone, page is paging, and time_from/to is reserved for use.*/
        static string url = "https://s.search.bilibili.com/cate/search?main_ver=v3&search_type=video" +
            "&view_type=hot_rank&cate_id=30&pagesize=20&time_from={0}&time_to={1}&page=";
        //an API for getting information such as video playback based on the 'aid'
        static string oneurl = "http://api.bilibili.com/archive_stat/stat?aid=";
        static int pages = 1;//to be the last parameter of 'url'

        public static MySqlConnection dbConnection;//database connection
        static string host = "localhost";//host name 
        static string id = "root";  //your server account
        static string pwd = "123456";  //your server account
        static string database = "xxx";//your database name
        //personal  information of your connection 
        static string connectionString = string.Format("Server = {0}; Database = {1}; User ID = {2}; Password = {3};charset=utf8;Allow User Variables=True;",
            host, database, id, pwd);

        static string results = " ";//to be infomation of all video 
        static string result_one = " "; //to be infomation of single video
        static int i = 1;//to count the number of videos and get data automatically

        public static HttpWebRequest request;
        public static HttpWebResponse response;

        public static StringBuilder sql;

        static void Main(string[] args)
        {
            //connect to the database;
            openSqlConnection(connectionString);
            //new a table;
            try
            {
                newTable();
            }catch(Exception ex)
            {
                String message = ex.Message;
                Console.WriteLine(message);
            }
            //to avoid duplicates,clear up it each time
            clearTable();

            //to meet API's rules
            String today = DateTime.Now.ToString("yyyy-MM-dd");
            today = today.Replace("-", "");
            url = String.Format(url, today, today);

            sql = new StringBuilder();//init sql words

            Console.Write(results);
            while (results.Equals("5.5.5-10.1.19-MariaDB") || results.Length>500)//to start or end the program
            {
                handJson(getBilis(pages++));
            }
            //exit
            Console.ReadKey();
            OnApplicationQuit();
        }
        // Connect to database    
        private static void openSqlConnection(string connectionString)
        {
            dbConnection = new MySqlConnection(connectionString);
            dbConnection.Open();
            results = dbConnection.ServerVersion;  //get MySql's version  
            Console.WriteLine(results);
        }
        // Disconnect from database    
        private static void closeSqlConnection()
        {
            dbConnection.Close();
            dbConnection = null;
        }
        //add data in the database
        static void addData(String sql)
        {
            MySqlCommand Cmd = new MySqlCommand(sql, dbConnection);
            try
            {
                Cmd.ExecuteNonQuery();
                Console.WriteLine("添加数据成功了！");
            }
            catch (Exception ex)
            {
                String message = ex.Message;
                Console.WriteLine("添加数据失败了！" + message);
            }
        }

        static void newTable()
        {
            String today = DateTime.Now.ToString("yyyy-MM-dd"); 
            String todaySQL = "CREATE TABLE `vocaloid_everyday`.`vocaloid_"+today+"` "
                + "( `id` INT(12) NOT NULL AUTO_INCREMENT , `aid` VARCHAR(12) NOT NULL , "
                + "`title` VARCHAR(240) NOT NULL ,`pubdate` DATETIME NOT NULL ,   "
                + "`author` VARCHAR(45) NOT NULL ,`mid` VARCHAR(12) NOT NULL , "
                + "`pic` VARCHAR(100) NOT NULL , `view` VARCHAR(12) NOT NULL , "
                + "`danmaku` DOUBLE NOT NULL , `favorite` DOUBLE NOT NULL , "
                + "`reply` DOUBLE NOT NULL , `share` DOUBLE NOT NULL , "
                + "`coin` DOUBLE NOT NULL , `score` DOUBLE NOT NULL , PRIMARY KEY (`id`)) ENGINE = InnoDB; ";
            MySqlCommand Cmd = new MySqlCommand(todaySQL, dbConnection);
            try
            {
                Cmd.ExecuteNonQuery();
                Console.WriteLine("创建" + today + "数据表成功了！");
            }
            catch (Exception ex)
            {
                String message = ex.Message;
                Console.WriteLine("创建" + today + "数据表失败了！" + message);
            }

        }

        static void clearTable()
        {
            String today = DateTime.Now.ToString("yyyy-MM-dd");
            String delSQL = "TRUNCATE TABLE `vocaloid_"+today+"`";

            MySqlCommand Cmd = new MySqlCommand(delSQL, dbConnection);
            try
            {
                Cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                String message = ex.Message;
            }
        }
        //to get information of all videos
        static string getBilis(int pages)
        {
            results = null;
            request = (HttpWebRequest)HttpWebRequest.Create(url + pages);
            request.Timeout = 3000;
            request.Method = "GET";
            request.UserAgent = "User-Agent:Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2; .NET CLR 1.0.3705";
            request.Accept = "*/*";
            request.Headers.Add("Accept-Language", "zh-cn,en-us;q=0.5");
            response = (HttpWebResponse)request.GetResponse();
            StreamReader sr = new StreamReader(response.GetResponseStream());
            results = sr.ReadToEnd();
            response.Close();
            return results;
        }
        //to get detailed information for each video by 'aid'
        static string getBili(string aid)
        {
            result_one = null;
            request = (HttpWebRequest)HttpWebRequest.Create(oneurl + aid);
            request.Timeout = 3000;
            request.Method = "GET";
            request.UserAgent = "User-Agent:Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2; .NET CLR 1.0.3705";
            request.Accept = "*/*";
            request.Headers.Add("Accept-Language", "zh-cn,en-us;q=0.5");
            response = (HttpWebResponse)request.GetResponse();
            StreamReader sr = new StreamReader(response.GetResponseStream());
            result_one = sr.ReadToEnd();
            response.Close();
            return result_one;
        }
        //data processing
        static void handJson(string results)
        {
            sql.Clear();
            //sql.Append("START TRANSACTION; ");

            Infos infos = JsonConvert.DeserializeObject<Infos>(results);

            foreach (result res in infos.result)
            {
                String today = DateTime.Now.ToString("yyyy-MM-dd"); ;

                string temp = "INSERT INTO  `vocaloid_" + today + "`(`aid`, `title`,  `pubdate`,`author`, `mid`, " +
                    " `pic`, `view`, `danmaku`, `favorite`, `reply`, `share`, `coin`, `score`)" +
                    "VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}'); ";

                result_one = getBili(res.id);//to get detailed information for each video by 'aid'
                if (result_one.Length < 100)//determine if the video exists
                {
                    string some = String.Format(temp, res.id, res.title.Replace("'", "''"), res.pubdate, res.author.Replace("'", "''"), res.mid, "http:" + res.pic,
                    0, 0, 0, 0, 0, 0, 0);

                    Console.WriteLine(i);
                    Console.WriteLine("av号：" + res.id);
                    Console.WriteLine("标题：" + res.title);
                    Console.WriteLine("作者：" + res.author);

                    sql.Append(some);
                }
                else
                {
                    List<Info> infoList = JsonConvert.DeserializeObject<List<Info>>("[" + result_one + "]");

                    foreach (Info info in infoList)
                    {
                        double play = Double.Parse(info.data.view.Replace("--", "0"));
                        double comm = info.data.reply;
                        double danmu = info.data.danmaku;
                        double coll = info.data.favorite;
                        double xiuA, xiuB, points;
                        xiuB = (coll / play) * 250;
                        if (play > 10000) play = play * 0.5 + 5000;                       
                        if (xiuB > 50) xiuB = 50;
                        if (xiuB < 10) play = play * xiuB * 0.1;
                        xiuA = (play + coll) / (play + coll + danmu * 10 + comm * 20);
                        points = Math.Round(play + (comm * 25 + danmu) * xiuA + coll * xiuB, 2);

                        //format sql words
                        string some = String.Format(temp, res.id, res.title.Replace("'", "''"), res.pubdate, res.author.Replace("'", "''"), res.mid, "http:" + res.pic,
                        info.data.view.ToString(), info.data.danmaku, info.data.favorite, info.data.reply, info.data.share, info.data.coin, points);

                        Console.WriteLine(i);
                        Console.WriteLine("av号：" + res.id);
                        Console.WriteLine("标题：" + res.title);
                        Console.WriteLine("作者：" + res.author);

                        sql.Append(some);
                    }
                }               
                
                i++;
            }
            //sql.Append("COMMIT; ");
            addData(sql.ToString());
        }
        // On quit    
        public static void OnApplicationQuit()
        {
            closeSqlConnection();
        }

        //the following is for the analysis of Json got by API
        public class Infos
        {
            public string numPages { get; set; }
            public List<result> result { get; set; }
        }

        //[DataContract]
        public class result
        {
            //[DataMember]
            public string id { get; set; }
            // [DataMember]
            public string author { get; set; }
            //[DataMember]
            public string pubdate { get; set; }
            // [DataMember]
            public string description { get; set; }
            //[DataMember]
            public string mid { get; set; }
            // [DataMember]
            public string pic { get; set; }
            // [DataMember]
            public string title { get; set; }
        }

        public class Info
        {
            public string code { get; set; }
            public data data { get; set; }
            public string message { get; set; }
            public string ttl { get; set; }
        }

        //[DataContract]
        public class data
        {
            //[DataMember]
            public string aid { get; set; }
            // [DataMember]
            public string view { get; set; }
            // [DataMember]
            public double danmaku { get; set; }
            //[DataMember]
            public double reply { get; set; }
            //[DataMember]
            public double favorite { get; set; }
            //[DataMember]
            public double coin { get; set; }
            //[DataMember]
            public double share { get; set; }
            // [DataMember]
            public int now_rank { get; set; }
            //[DataMember]
            public int his_rank { get; set; }
            //[DataMember]
            public int like { get; set; }
            //[DataMember]
            public string no_reprint { get; set; }
            //[DataMember]
            public string copyright { get; set; }
        }
    }
}
