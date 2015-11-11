package nba;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.firefox.FirefoxDriver;

public class NbaGameIdScraper {

	private static Connection connection;
	private static Statement stmt = null;
	private static ResultSet rs = null;
	private static PreparedStatement ps = null;
	private static void scrape() throws SQLException, SlowJavascriptException{
		rs = stmt.executeQuery("select game_date, count(*) as count from matches where playoffyear in (2014,2015) and not exists (select * from sportsvugameids where sportsvugameids.game_date = matches.game_date) group by game_date order by game_date");
		while(rs.next()){
			java.sql.Date gameDate = rs.getDate("game_date");
			int brGameCount = rs.getInt("count");
			String[] gameDateArray = gameDate.toString().split("-");
			String year = gameDateArray[0];
			String month = gameDateArray[1];
			String day = gameDateArray[2];
			String nbaWebsiteDate = month+"/"+day+"/"+year;
			String website = "http://stats.nba.com/scores/?ls=iref:nba:gnav#!/"+nbaWebsiteDate;
			int sportsVuGameCount = 0;
			ArrayList<String> gameIds = new ArrayList<String>();
			Set<String> set = new HashSet<String>(gameIds);
			while(set.size() != brGameCount && set.size() == 0){
				WebDriver driver = null;
				driver = new FirefoxDriver();
				driver.get(website);
				//Thread.sleep(5000L);
				Document document = Jsoup.parse(driver.getPageSource());
				driver.quit();
				Elements games = document.select("div.game");
				for(Element game : games){
					String link = game.select("a.game-footer__pt").attr("href");
					String gameIdSportsVu = link.split("/")[3];
					ps.setString(1, gameIdSportsVu);
					ps.setDate(2, gameDate);
					ps.executeUpdate();
					connection.commit();
					System.out.println(gameIdSportsVu);
					gameIds.add(gameIdSportsVu);
					//sportsVuGameCount++;
				}
				System.out.println(website);
				set = new HashSet<String>(gameIds);
				if(set.size() != brGameCount){
					System.out.println("Slow javascript!");
				}
			}
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void main(String [] args){
		try {
			// Define things for DriverManager.getConnection
			String url = "jdbc:mysql://localhost:3306/";
			String dbName = "NBA";
			String userName = "root"; 
			String password = "";
			connection = DriverManager.getConnection(url+dbName,userName,password);
			connection.setAutoCommit(false);
			stmt = connection.createStatement();
			ps = connection.prepareStatement("insert into sportsvugameids(game_id, game_date) values(?, ?)");
			connection.commit();
			scrape();
			System.out.println("Finished.");
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
}
