import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class JDBCConnectivity {

	public static void main(String[] args) 
	{
		String path = "oracle.jdbc.driver.OracleDriver"	;
		String url = "jdbc:oracle:thin:@localhost:1521:xe";
		String user = "system";
		String password = "system";
		
		String sql1 = "SELECT EXTRACT(HOUR FROM START_TIME), EXTRACT(HOUR FROM END_TIME) FROM (SELECT  START_TIME, END_TIME FROM SAMPLE_DATA GROUP BY START_TIME , END_TIME ORDER BY count(START_TIME) desc) where rownum<2";
		String sql2 = "SELECT EXTRACT(HOUR FROM START_TIME), EXTRACT(HOUR FROM END_TIME) FROM SAMPLE_DATA WHERE DURATION = (SELECT MAX(DURATION) FROM SAMPLE_DATA)";
		String sql3 = "SELECT EXTRACT(DAY FROM START_TIME), EXTRACT(DAY FROM END_TIME) FROM (SELECT  START_TIME, END_TIME FROM SAMPLE_DATA GROUP BY START_TIME , END_TIME ORDER BY count(START_TIME) desc)where rownum<2";
		String sql4 = "SELECT EXTRACT(DAY FROM START_TIME), EXTRACT(DAY FROM END_TIME) FROM SAMPLE_DATA WHERE DURATION = (SELECT MAX(DURATION) FROM SAMPLE_DATA)";
		
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		
		
		try
		{
			Class.forName(path);
			con = DriverManager.getConnection(url, user, password);
			st = con.createStatement();
			//Query to analyze for longer call in a day
			rs = st.executeQuery(sql1);
			
			while(rs.next()==true)
			{
				int start = rs.getInt(1);
				System.out.println("HOUR OF DAY WHEN CALL VALUEM IS HIGHEST IS  "+start+" AND "+(start+1)+" AM");
				System.out.println("**********************************************");
			}
			
			//Query to analyze highest volume hour in a day 
			st = con.createStatement();
			rs = st.executeQuery(sql2);
			
			while(rs.next()==true)
			{
				
				int start_time = rs.getInt(1);
				int end_time = rs.getInt(2);
				System.out.println("HOUR OF DAY WHEN CALLS ARE LONGER IS  "+start_time+" AND "+(end_time+1)+" AM");
				System.out.println("**********************************************");
			}
			
			//Query to analyze day in a week which has highest volume 
			st = con.createStatement();
			rs = st.executeQuery(sql3);
			while(rs.next()==true)
			{
				int start1 = rs.getInt(1);
				System.out.println("DAY WHICH HAS HIGHEST CALL VALUEM IS  "+ start1);
				System.out.println("**********************************************");
			}
			
			//Query to analyze day in a week which has longest call
			st = con.createStatement();
			rs = st.executeQuery(sql4);
			while(rs.next()==true)
			{
				int start_time1 = rs.getInt(1);
				System.out.println("DAY AT WHICH HAS LONGEST CALL  "+ start_time1);
				System.out.println("**********************************************");
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}

}
