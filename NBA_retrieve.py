
# coding: utf-8

# In[62]:



    
    
    

import pandas as pd
import urllib3
import json as js
from datetime import datetime as dt
from datetime import timedelta, date
import time
from pathlib import Path
import cx_Oracle
# Get Files
urllib3.disable_warnings()

# scores.

from datetime import date, timedelta
yesterday = date.today() - timedelta(1)
yesterday_str = yesterday.strftime("%Y-%m-%d")

#yesterday_str = "2018-10-16"

url_base_nfl_url = "https://ca.global.nba.com/stats2/season/schedule.json?countryCode=CA&gameDate=" + yesterday_str+ "&locale=en&tz=-5"
http = urllib3.PoolManager()

# Using "GET" method of web page retrieval, retrieve the page
response = http.request("GET", url_base_nfl_url)

# Load data into JSON Object.

json_base_data = js.loads(response.data)

import time
import os

os.environ["ORACLE_HOME"] = "/opt/app/oracle/product/12.1.0/dbhome_1"
os.environ["LD_LIBRARY_PATH"] = "/opt/app/oracle/product/12.1.0/dbhome_1/lib"
con = cx_Oracle.connect('dkings/dkings@192.168.1.113/dfsdb')
cur = con.cursor()
all_dates = json_base_data["payload"]["dates"]
game_data_list = []
team_data_list = []
game_dates = []
for all_games in (all_dates):
    for game in (all_games["games"]):
        if (game["boxscore"]["status"] == "3"): # game is final
            game_data = ()
            team_data = ()
            timestamp = (time.strftime("%Y%m%d%H%M%S", time.localtime(float(game["profile"]["utcMillis"])/1000)) )
            game_data = (timestamp, game["profile"]["awayTeamId"], game["profile"]["homeTeamId"] ,  game["profile"]["utcMillis"] , game["boxscore"]["awayScore"], game["boxscore"]["homeScore"], game["profile"]["gameId"])
            team_data = (game["homeTeam"]["profile"]["id"],  game["homeTeam"]["profile"]["abbr"],game["homeTeam"]["profile"]["conference"],game["homeTeam"]["profile"]["division"],)
            cur.execute("DELETE FROM DKINGS.NBA_V2_GAMES WHERE GAMEDATE =  " + timestamp + " AND HOMETEAMID = " +  game["profile"]["homeTeamId"])
            cur.execute("DELETE FROM DKINGS.NBA_V2_PBP WHERE GAMEID =  '" + game["profile"]["gameId"] + "'")
            cur.execute("DELETE FROM DKINGS.NBA_V2_SNAPSHOT WHERE GAMEID =  '" + game["profile"]["gameId"] + "'")
            
            con.commit()
            cur.executemany("INSERT INTO DKINGS.NBA_V2_GAMES(GAMEDATE, AWAYTEAMID, HOMETEAMID, UTCMILLIS, AWAYSCORE, HOMESCORE, GAMEID) VALUES(:1, :2, :3, :4, :5, :6, :7)", [game_data])
            con.commit()

            merge_sql = "MERGE INTO DKINGS.NBA_V2_TEAMS T USING (SELECT " + team_data[0] + " AS TEAMID, '" + team_data[1]+                          "' as TEAM  , '" + team_data[2] +"' as CONFERENCE, '"+ team_data[3]+                         "' AS DIVISION FROM DUAL) src on (src.teamid = t.teamid) WHEN NOT MATCHED THEN INSERT (TEAMID, TEAM, CONFERENCE, DIVISION) VALUES(SRC.TEAMID, SRC.TEAM, SRC.CONFERENCE, SRC.DIVISION)"

            #print(merge_sql)
            cur.execute(merge_sql)
            con.commit()
            
            # Get PBP data.
            
            pbp_url = "https://ca.global.nba.com/stats2/game/playbyplay.json?gameId="+ game["profile"]["gameId"] +"&locale=en&period=4"
            
            # Using "GET" method of web page retrieval, retrieve the page
            response = http.request("GET", pbp_url)

            # Load data into JSON Object.
            pbp_json_data = js.loads(response.data)
            pbp_events = pbp_json_data["payload"]["playByPlays"][0]["events"]

            play_list = []
            for event in (pbp_events):
                msgtype = 0
                msg = event["description"].encode("utf-8").decode("ascii","ignore")
                
                play = (game["profile"]["gameId"],event["teamId"] , event["awayScore"], msg, event["displayPlayerId"], event["gameClock"], event["homeScore"], 
                        event["locationX"], event["locationY"],
                       msgtype, event["offensiveTeamId"], event["period"], event["playerId"], event["playerId2"], event["playerId3"], event["points"], event["statCategory"], 
                       event["statCategory2"], event["statValue"], event["statValue2"], event["statValue3"])
                play_list.append(play)

            #print(play_list)
            
            cur.executemany("INSERT INTO DKINGS.NBA_V2_PBP( GAMEID,TEAMID,AWAYSCORE,DESCRIPTION,OFFENSIVEPLAYERID," + 
                                                            "GAMECLOCK,HOMESCORE,LOCATIONX,LOCATIONY,MESSAGETYPE," +  
                                                            "OFFENSIVETEAMID,PERIOD,PLAYERID,PLAYERID2,PLAYERID3," + 
                                                            "POINTS,STATCATEGORY,STATCATEGORY2,STATVALUE,STATVALUE2,STATVALUE3) VALUES(" + 
                                                            ":1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11,"  + 
                                                            ":12, :13, :14, :15, :16, :17, :18, :19, :20, :21)", play_list) 
            con.commit()



            # Get PBP data.
            snapshot_url = "https://ca.global.nba.com/stats2/game/snapshot.json?countryCode=CA&gameId="+ game["profile"]["gameId"]  +"&locale=en&tz=-5"


            # Using "GET" method of web page retrieval, retrieve the page
            response = http.request("GET", snapshot_url)
            

            snapshot_json = js.loads(response.data)
            hometeamid = snapshot_json["payload"]["homeTeam"]["profile"]["id"]
            awayteamid = snapshot_json["payload"]["awayTeam"]["profile"]["id"]
            playerstats = []
            for player in (snapshot_json["payload"]["homeTeam"]["gamePlayers"]):
                pstats = () 
                stats = player["statTotal"]
                pstats = ( game["profile"]["gameId"], hometeamid, player["profile"]["playerId"], player["profile"]["experience"], player["profile"]["firstName"], player["profile"]["lastName"],
                         player["profile"]["height"], player["profile"]["position"], player["profile"]["weight"], player["boxscore"]["isStarter"], player["boxscore"]["plusMinus"],
                         stats["assists"], stats["blocks"], stats["defRebs"], stats["fga"], stats["fgm"], stats["fouls"], stats["fta"], stats["ftm"],
                         stats["mins"], stats["offRebs"], stats["points"], stats["secs"], stats["steals"], stats["turnovers"], player["profile"]["leagueId"], "H", stats["tpa"], stats["tpm"])
                playerstats.append(pstats)
            #print(pstats)
            cur.executemany("INSERT INTO DKINGS.NBA_V2_SNAPSHOT(GAMEID, TEAMID, PLAYERID,"+
                            " EXPERIENCE, FIRSTNAME, LASTNAME, HEIGHT, POSITION, WEIGHT, IS_STARTER,"+
                            " PLUSMINUS, AST, BLK, DEFREB, FGA, FGM, FOUL, FTA, FTM, MINS, OFFREBS, PTS, SECS, STEALS, TURNOVERS, LEAGUEID, GAMELOC, TPA, TPM) " + 
                           "VALUES(:1, :2,:3, :4, :5, :6, :7,:8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29)", playerstats)
            con.commit()
            playerstats = []
            for player in (snapshot_json["payload"]["awayTeam"]["gamePlayers"]):
                pstats = () 
                stats = player["statTotal"]
                pstats = ( game["profile"]["gameId"], awayteamid, player["profile"]["playerId"], player["profile"]["experience"], player["profile"]["firstName"], player["profile"]["lastName"],
                         player["profile"]["height"], player["profile"]["position"], player["profile"]["weight"], player["boxscore"]["isStarter"], player["boxscore"]["plusMinus"],
                         stats["assists"], stats["blocks"], stats["defRebs"], stats["fga"], stats["fgm"], stats["fouls"], stats["fta"], stats["ftm"],
                         stats["mins"], stats["offRebs"], stats["points"], stats["secs"], stats["steals"], stats["turnovers"], player["profile"]["leagueId"],"V", stats["tpa"], stats["tpm"])
                playerstats.append(pstats)
            #print(pstats)
            cur.executemany("INSERT INTO DKINGS.NBA_V2_SNAPSHOT(GAMEID, TEAMID, PLAYERID,"+
                            " EXPERIENCE, FIRSTNAME, LASTNAME, HEIGHT, POSITION, WEIGHT, IS_STARTER,"+
                            " PLUSMINUS, AST, BLK, DEFREB, FGA, FGM, FOUL, FTA, FTM, MINS, OFFREBS, PTS, SECS, STEALS, TURNOVERS, LEAGUEID, GAMELOC, TPA, TPM) " + 
                           "VALUES(:1, :2,:3, :4, :5, :6, :7,:8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29)", playerstats)
            con.commit()
            


cur.execute("UPDATE DKINGS.NBA_V2_SNAPSHOT S SET " + 
            "DK_PTS = " +
            "(COALESCE(S.PTS,0)) + (S.TPM * 0.5) + (1.25 * COALESCE((S.OFFREBS + S.DEFREB),0)) + (1.5 * S.AST) + (2*S.BLK) + (2*S.STEALS) + (-0.5 * S.TURNOVERS) + " +
            "COALESCE( " + 
            "CASE WHEN (     ( CASE WHEN S.PTS >= 10 THEN 1 ELSE 0 END ) +(CASE WHEN ( S.OFFREBS + S.DEFREB ) >= 10 THEN 1 ELSE 0 END) + (CASE WHEN S.AST >= 10 THEN 1 ELSE 0 END) + "+
            "(CASE WHEN S.BLK >= 10 THEN 1 ELSE 0 END) + ( CASE WHEN S.STEALS >= 10 THEN 1 ELSE 0 END) )  = 2 THEN 1.5 WHEN "  + 
            "( ( CASE WHEN S.PTS >= 10 THEN 1 ELSE 0 END ) + (CASE WHEN ( S.OFFREBS + S.DEFREB ) >= 10 THEN 1 ELSE 0 END) + " +
            "(CASE WHEN S.AST >= 10 THEN 1 ELSE 0 END) + (CASE WHEN S.BLK >= 10 THEN 1 ELSE 0 END) + " +
            "( CASE WHEN S.STEALS >= 10 THEN 1 ELSE 0 END) ) = 3 THEN 3 END ,0) ")
con.commit()

cur.execute("MERGE INTO DKINGS.NBA_V2_SNAPSHOT TGT USING ( " + 
            "WITH TAB_TM AS (SELECT GAMEID, TEAMID, SUM(MINS) TOT_MINS, SUM(FGA) TOT_FGA, SUM(FTA) TOT_FTA, SUM(TURNOVERS) TOT_TOV FROM DKINGS.NBA_V2_SNAPSHOT GROUP BY GAMEID, TEAMID) " + 
            "SELECT S.PLAYERID, S.GAMEID, S.TEAMID, 100* " + 
            "CASE WHEN (S.MINS * (TAB_TM.TOT_FGA + (0.44 * TAB_TM.TOT_FTA) + TAB_TM.TOT_TOV))> 0 THEN " + 
            "((FGA + (0.44*FTA) + TURNOVERS) * ((TAB_TM.TOT_MINS)/5))/(S.MINS * (TAB_TM.TOT_FGA + (0.44 * TAB_TM.TOT_FTA) + TAB_TM.TOT_TOV))  " + 
            "ELSE 0 END USAGE_RATE, " + 
            "FGA+AST+DEFREB+OFFREBS+(FTA/2)+STEALS  POSSESSION  " + 
            "FROM DKINGS.NBA_V2_SNAPSHOT S " + 
            "INNER JOIN TAB_TM ON TAB_TM.GAMEID = S.GAMEID AND TAB_TM.TEAMID = S.TEAMID  " + 
            ") SRC ON (TGT.GAMEID = SRC.GAMEID AND TGT.TEAMID = SRC.TEAMID AND TGT.PLAYERID = SRC.PLAYERID) " + 
            "WHEN MATCHED THEN UPDATE " + 
            "SET TGT.USAGE_RATE = SRC.USAGE_RATE, POSSESSION = SRC.POSSESSION") 
con.commit()
con.close()




import pandas as pd
import urllib3
import json as js
from datetime import datetime as dt
from datetime import timedelta, date
import time
from pathlib import Path
import cx_Oracle
import dateutil.parser
import os
from pytz import timezone

os.environ["ORACLE_HOME"] = "/opt/app/oracle/product/12.1.0/dbhome_1"
os.environ["LD_LIBRARY_PATH"] = "/opt/app/oracle/product/12.1.0/dbhome_1/lib"

# Get Files
urllib3.disable_warnings()

# scores.
url_base_nfl_url = "https://www.draftkings.com/lobby/getcontests?sport=NBA"
http = urllib3.PoolManager()

# Using "GET" method of web page retrieval, retrieve the page
response = http.request("GET", url_base_nfl_url)

# Load data into JSON Object.

json_base_data = js.loads(response.data)

dg = []
for x in (json_base_data["Contests"]):
    if x["gameType"] == "Classic":
        if x["dg"] not in dg:
            dg.append(x["dg"])
players=[]

con = cx_Oracle.connect('dkings/dkings@192.168.1.113/dfsdb')
compdates=[]
for y in (dg):
    #https://api.draftkings.com/draftgroups/v1/draftgroups/21434/draftables?format=json
    
    dg_url = "https://api.draftkings.com/draftgroups/v1/draftgroups/" + str(y) +"/draftables?format=json"
    print(dg_url)
    dg_response = http.request("GET",dg_url)
    dg_json = js.loads(dg_response.data)

    for p in (dg_json["draftables"]):
        player=()
        comp_date = p["competition"]["startTime"]
        start_date_tmp = dateutil.parser.parse(comp_date)
        start_date = start_date_tmp.astimezone(timezone("Canada/Eastern"))
        #cdate = comp_date[0:4] + comp_date[5:7]+comp_date[8:10]
        cdate = start_date.strftime("%Y%m%d")
        if cdate not in compdates:
            compdates.append(cdate)
        firstname = p["firstName"].encode("utf-8").decode("ascii","ignore")
        lastname = p["lastName"].encode("utf-8").decode("ascii","ignore")
        player=(cdate,p["competition"]["name"],firstname,lastname,p["playerId"],p["position"],p["salary"],p["teamAbbreviation"])
        if player not in players:
            players.append(player)


cur = con.cursor()

for c in (compdates):
    cur.execute("DELETE FROM DKINGS.NBA_V2_SALARY WHERE GAMEDATE = " + str(c))
    
cur.executemany("INSERT INTO DKINGS.NBA_V2_SALARY(GAMEDATE, GAME_NAME, FIRSTNAME, LASTNAME, PLAYERID, POSITION, SALARY, TEAMNAME) VALUES(:1, :2, :3, :4, :5, :6, :7,:8)", players)

con.commit()
con.close()
