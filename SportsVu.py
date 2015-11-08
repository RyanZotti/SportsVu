import requests
import pandas as pd
import pymysql
con = pymysql.connect(host='localhost', unix_socket='/tmp/mysql.sock', user='root', passwd="", db='NBA')
mysql = con.cursor(pymysql.cursors.DictCursor)

mysql.execute("""
    create temporary table already_scraped
    (index idx1 (game_id))
    select game_id from SportsVuMomentsRaw group by game_id""")
mysql.execute("""
    select game_id from SportsVuGameIds where not exists 
    (select * from already_scraped where already_scraped.game_id = SportsVuGameIds.game_id)""")
for row in mysql.fetchall():
    game_id=row['game_id']
    for event_id in range(1,1000):
        url = "http://stats.nba.com/stats/locations_getmoments/?eventid={event_id}&gameid={game_id}".format(event_id=event_id,game_id=game_id)
        response = requests.get(url)
        if response.status_code == 200:
            home = response.json()["home"]
            away = response.json()["visitor"]
            moments = response.json()["moments"]
            home_name = home['name']
            home_id = home['teamid']
            away_id = away['teamid']
            away_name = away['name']
            home_players = pd.DataFrame(home['players'])
            away_players = pd.DataFrame(away['players'])
            for moment in moments:
                quarter = moment[0]
                game_time_remaining = moment[2]
                shot_clock_time = moment[3]
                player_moments = pd.DataFrame(moment[5],columns=['team_id','player_id','x','y','ball_radius'])
                ball_moment = player_moments.loc[(player_moments.team_id==-1)&(player_moments.player_id==-1)]
                if(len(ball_moment) == 0):
                    ball_moment = ball_moment.append({'x':-1,'y':-1,'ball_radius':-1},ignore_index=True)
                player_moments_home = player_moments.loc[player_moments['team_id'] == home['teamid']]
                player_moments_home = player_moments_home.sort(columns=['player_id'],ascending=True)
                player_moments_home = player_moments_home.merge(home_players,left_on=['player_id'],right_on=['playerid'])
                player_moments_home['fullname'] = player_moments_home['firstname']+" "+player_moments_home['lastname']
                player_moments_away = player_moments.loc[player_moments['team_id'] == away['teamid']]
                player_moments_away = player_moments_away.sort(columns=['player_id'],ascending=True)
                player_moments_away = player_moments_away.merge(away_players,left_on=['player_id'],right_on=['playerid'])
                player_moments_away['fullname'] = player_moments_away['firstname']+" "+player_moments_away['lastname']
                if len(player_moments_home) != 5 or len(player_moments_away) != 5:
                    continue
                player_moments_home.index = range(1,6)
                player_moments_away.index = range(1,6)
                mysql.execute("""insert into SportsVuMomentsRaw(game_id, home, home_id, away, away_id, event_id, unknown1, quarter, game_time_remaining, shot_clock_time, unknown2, ball_x, ball_y, ball_radius, h1_id, h1_x, h1_y, h1_radius, h2_id, h2_x, h2_y, h2_radius, h3_id, h3_x, h3_y, h3_radius, h4_id, h4_x, h4_y, h4_radius, h5_id, h5_x, h5_y, h5_radius, a1_id, a1_x, a1_y, a1_radius, a2_id, a2_x, a2_y, a2_radius, a3_id, a3_x, a3_y, a3_radius, a4_id, a4_x, a4_y, a4_radius, a5_id, a5_x, a5_y, a5_radius, h1_first_name, h1_last_name, h1_full_name, h1_jersey, h1_position, h2_first_name, h2_last_name, h2_full_name, h2_jersey, h2_position, h3_first_name, h3_last_name, h3_full_name, h3_jersey, h3_position, h4_first_name, h4_last_name, h4_full_name, h4_jersey, h4_position, h5_first_name, h5_last_name, h5_full_name, h5_jersey, h5_position, a1_first_name, a1_last_name, a1_full_name, a1_jersey, a1_position, a2_first_name, a2_last_name, a2_full_name, a2_jersey, a2_position, a3_first_name, a3_last_name, a3_full_name, a3_jersey, a3_position, a4_first_name, a4_last_name, a4_full_name, a4_jersey, a4_position, a5_first_name, a5_last_name, a5_full_name, a5_jersey, a5_position) values("{game_id}","{home}","{home_id}","{away}","{away_id}","{event_id}","{unknown1}","{quarter}","{game_time_remaining}","{shot_clock_time}","{unknown2}","{ball_x}","{ball_y}","{ball_radius}","{h1_id}","{h1_x}","{h1_y}","{h1_radius}","{h2_id}","{h2_x}","{h2_y}","{h2_radius}","{h3_id}","{h3_x}","{h3_y}","{h3_radius}","{h4_id}","{h4_x}","{h4_y}","{h4_radius}","{h5_id}","{h5_x}","{h5_y}","{h5_radius}","{a1_id}","{a1_x}","{a1_y}","{a1_radius}","{a2_id}","{a2_x}","{a2_y}","{a2_radius}","{a3_id}","{a3_x}","{a3_y}","{a3_radius}","{a4_id}","{a4_x}","{a4_y}","{a4_radius}","{a5_id}","{a5_x}","{a5_y}","{a5_radius}","{h1_first_name}","{h1_last_name}","{h1_full_name}","{h1_jersey}","{h1_position}","{h2_first_name}","{h2_last_name}","{h2_full_name}","{h2_jersey}","{h2_position}","{h3_first_name}","{h3_last_name}","{h3_full_name}","{h3_jersey}","{h3_position}","{h4_first_name}","{h4_last_name}","{h4_full_name}","{h4_jersey}","{h4_position}","{h5_first_name}","{h5_last_name}","{h5_full_name}","{h5_jersey}","{h5_position}","{a1_first_name}","{a1_last_name}","{a1_full_name}","{a1_jersey}","{a1_position}","{a2_first_name}","{a2_last_name}","{a2_full_name}","{a2_jersey}","{a2_position}","{a3_first_name}","{a3_last_name}","{a3_full_name}","{a3_jersey}","{a3_position}","{a4_first_name}","{a4_last_name}","{a4_full_name}","{a4_jersey}","{a4_position}","{a5_first_name}","{a5_last_name}","{a5_full_name}","{a5_jersey}","{a5_position}")""".format(game_id=game_id,home=home_name,home_id=home_id,away=away_name,away_id=away_id,event_id=event_id,unknown1=moment[1],quarter=quarter,game_time_remaining=game_time_remaining,shot_clock_time=shot_clock_time,unknown2=moment[4],
                    ball_x=ball_moment.ix[0,'x'],ball_y=ball_moment.ix[0,'y'],ball_radius=ball_moment.ix[0,'ball_radius'],
                    h1_id=player_moments_home.ix[1,'player_id'],h1_x=player_moments_home.ix[1,'x'],h1_y=player_moments_home.ix[1,'y'],h1_radius=player_moments_home.ix[1,'ball_radius'],h1_first_name=player_moments_home.ix[1,'firstname'],h1_last_name=player_moments_home.ix[1,'lastname'],h1_full_name=player_moments_home.ix[1,'fullname'],h1_jersey=player_moments_home.ix[1,'jersey'],h1_position=player_moments_home.ix[1,'position'],
                    h2_id=player_moments_home.ix[2,'player_id'],h2_x=player_moments_home.ix[2,'x'],h2_y=player_moments_home.ix[2,'y'],h2_radius=player_moments_home.ix[2,'ball_radius'],h2_first_name=player_moments_home.ix[2,'firstname'],h2_last_name=player_moments_home.ix[2,'lastname'],h2_full_name=player_moments_home.ix[2,'fullname'],h2_jersey=player_moments_home.ix[2,'jersey'],h2_position=player_moments_home.ix[2,'position'],
                    h3_id=player_moments_home.ix[3,'player_id'],h3_x=player_moments_home.ix[3,'x'],h3_y=player_moments_home.ix[3,'y'],h3_radius=player_moments_home.ix[3,'ball_radius'],h3_first_name=player_moments_home.ix[3,'firstname'],h3_last_name=player_moments_home.ix[3,'lastname'],h3_full_name=player_moments_home.ix[3,'fullname'],h3_jersey=player_moments_home.ix[3,'jersey'],h3_position=player_moments_home.ix[3,'position'],
                    h4_id=player_moments_home.ix[4,'player_id'],h4_x=player_moments_home.ix[4,'x'],h4_y=player_moments_home.ix[4,'y'],h4_radius=player_moments_home.ix[4,'ball_radius'],h4_first_name=player_moments_home.ix[4,'firstname'],h4_last_name=player_moments_home.ix[4,'lastname'],h4_full_name=player_moments_home.ix[4,'fullname'],h4_jersey=player_moments_home.ix[4,'jersey'],h4_position=player_moments_home.ix[4,'position'],
                    h5_id=player_moments_home.ix[5,'player_id'],h5_x=player_moments_home.ix[5,'x'],h5_y=player_moments_home.ix[5,'y'],h5_radius=player_moments_home.ix[5,'ball_radius'],h5_first_name=player_moments_home.ix[5,'firstname'],h5_last_name=player_moments_home.ix[5,'lastname'],h5_full_name=player_moments_home.ix[5,'fullname'],h5_jersey=player_moments_home.ix[5,'jersey'],h5_position=player_moments_home.ix[5,'position'],
                    a1_id=player_moments_away.ix[1,'player_id'],a1_x=player_moments_away.ix[1,'x'],a1_y=player_moments_away.ix[1,'y'],a1_radius=player_moments_home.ix[1,'ball_radius'],a1_first_name=player_moments_away.ix[1,'firstname'],a1_last_name=player_moments_away.ix[1,'lastname'],a1_full_name=player_moments_away.ix[1,'fullname'],a1_jersey=player_moments_away.ix[1,'jersey'],a1_position=player_moments_away.ix[1,'position'],
                    a2_id=player_moments_away.ix[2,'player_id'],a2_x=player_moments_away.ix[2,'x'],a2_y=player_moments_away.ix[2,'y'],a2_radius=player_moments_home.ix[2,'ball_radius'],a2_first_name=player_moments_away.ix[2,'firstname'],a2_last_name=player_moments_away.ix[2,'lastname'],a2_full_name=player_moments_away.ix[2,'fullname'],a2_jersey=player_moments_away.ix[2,'jersey'],a2_position=player_moments_away.ix[2,'position'],
                    a3_id=player_moments_away.ix[3,'player_id'],a3_x=player_moments_away.ix[3,'x'],a3_y=player_moments_away.ix[3,'y'],a3_radius=player_moments_home.ix[3,'ball_radius'],a3_first_name=player_moments_away.ix[3,'firstname'],a3_last_name=player_moments_away.ix[3,'lastname'],a3_full_name=player_moments_away.ix[3,'fullname'],a3_jersey=player_moments_away.ix[3,'jersey'],a3_position=player_moments_away.ix[3,'position'],
                    a4_id=player_moments_away.ix[4,'player_id'],a4_x=player_moments_away.ix[4,'x'],a4_y=player_moments_away.ix[4,'y'],a4_radius=player_moments_home.ix[4,'ball_radius'],a4_first_name=player_moments_away.ix[4,'firstname'],a4_last_name=player_moments_away.ix[4,'lastname'],a4_full_name=player_moments_away.ix[4,'fullname'],a4_jersey=player_moments_away.ix[4,'jersey'],a4_position=player_moments_away.ix[4,'position'],
                    a5_id=player_moments_away.ix[5,'player_id'],a5_x=player_moments_away.ix[5,'x'],a5_y=player_moments_away.ix[5,'y'],a5_radius=player_moments_home.ix[5,'ball_radius'],a5_first_name=player_moments_away.ix[5,'firstname'],a5_last_name=player_moments_away.ix[5,'lastname'],a5_full_name=player_moments_away.ix[5,'fullname'],a5_jersey=player_moments_away.ix[5,'jersey'],a5_position=player_moments_away.ix[5,'position']))
                con.commit()
    print(game_id)
con.close()
