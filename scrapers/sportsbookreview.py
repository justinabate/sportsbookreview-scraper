from datetime import datetime
import requests
import pandas as pd
from itertools import tee
import json
import io
import os

class OddsScraper:
    def __init__(self, sport, years):
        self.blacklist = [
            "pk",
            "PK",
            "NL",
            "nl",
            "a100",
            "a100",
            "a105",
            "a105",
            "a110",
            "a110",
            ".5+03",
            ".5ev",
            "-",
        ]
        self.sport = sport
        self.translator = json.load(open("config/translated.json", "r"))
        self.seasons = years

    def _translate(self, name):
        return self.translator[self.sport].get(name, name)

    @staticmethod
    def _make_season(season):
        season = str(season)
        yr = season[2:]
        next_yr = str(int(yr) + 1)
        return f"{season}-{next_yr}"

    @staticmethod
    def _make_datestr(date, season, start=8, yr_end=12):
        date = str(date)
        if len(date) == 3:
          date = f"0{date}"
        month = date[:2]
        day = date[2:]

        if int(month) in range(start, yr_end + 1):
          datecode = f"{season}{month}{day}"
        else:
          datecode = f"{int(season+1)}{month}{day}"
        dt_object = datetime.strptime(datecode, '%Y%m%d').date()
        return dt_object.strftime('%Y-%m-%d')

    @staticmethod
    def _pairwise(iterable):
        a, b = tee(iterable)
        next(b, None)
        return zip(a, b)

    def driver(self):
        df = pd.DataFrame()
        for season in self.seasons:
            season_str = self._make_season(season)
            url = self.base + season_str

            # Sportsbookreview has scraper protection, so we need to set a user agent
            # to get around this.
            headers = {"User-Agent": "Mozilla/5.0"}
            r = requests.get(url, headers=headers)

            dfs = pd.read_html(r.text)
            df = pd.concat([df, self._reformat_data(dfs[0][1:], season)], axis=0)
        return self._to_schema(df)


class NFLOddsScraper(OddsScraper):
    def __init__(self, years):
        super().__init__("nfl", years)
        self.base = (
            "https://www.sportsbookreviewsonline.com/scoresoddsarchives/nfl-odds-"
        )
        self.schema = {
            "season": [],
            "date": [],
            "home_team": [],
            "away_team": [],
            "home_1stQtr": [],
            "away_1stQtr": [],
            "home_2ndQtr": [],
            "away_2ndQtr": [],
            "home_3rdQtr": [],
            "away_3rdQtr": [],
            "home_4thQtr": [],
            "away_4thQtr": [],
            "home_final": [],
            "away_final": [],
            "ML_H_cl_odds": [],
            "ML_A_cl_odds": [],
            "home_open_spread": [],
            "away_open_spread": [],
            "S_H_cl_line": [],
            "S_A_cl_line": [],
            "home_2H_spread": [],
            "away_2H_spread": [],
            "2H_total": [],
            "OU_op_line": [],
            "OU_cl_line": [],
        }

    def _reformat_data(self, df, season):
        new_df = pd.DataFrame()
        new_df["season"] = [season] * len(df)
        new_df["date"] = df[0].apply(lambda x: self._make_datestr(x, season))
        new_df["name"] = df[3]
        new_df["1stQtr"] = df[4]
        new_df["2ndQtr"] = df[5]
        new_df["3rdQtr"] = df[6]
        new_df["4thQtr"] = df[7]
        new_df["final"] = df[8]
        _open = df[9].apply(lambda x: 0 if x in self.blacklist else x)
        new_df["open_odds"] = _open
        close = df[10].apply(lambda x: 0 if x in self.blacklist else x)
        new_df["close_odds"] = close
        new_df["close_ml"] = df[11]
        h2 = df[12].apply(lambda x: 0 if x in self.blacklist else x)
        new_df["2H_odds"] = h2
        return new_df

    def _to_schema(self, df):
        new_df = self.schema.copy()
        df = df.fillna(0)
        progress = df.iterrows()
        # remove the first row, as it is the header
        next(progress)
        for (i1, row), (i2, next_row) in self._pairwise(progress):
            # skip every other row
            if i1 % 2 == 0:
                continue

            home_ml = int(next_row["close_ml"])
            away_ml = int(row["close_ml"])

            odds1 = float(row["open_odds"])
            odds2 = float(next_row["open_odds"])
            if odds1 < odds2:
                open_spread = odds1
                S_cl_line = float(row["close_odds"])
                h2_spread = float(row["2H_odds"])

                h2_total = float(next_row["2H_odds"])
                open_ou = odds2
                close_ou = float(next_row["close_odds"])
            else:
                open_spread = odds2
                S_cl_line = float(next_row["close_odds"])
                h2_spread = float(next_row["2H_odds"])

                h2_total = float(row["2H_odds"])
                open_ou = odds1
                close_ou = float(row["close_odds"])

            home_open_spread = -open_spread if home_ml < away_ml else open_spread
            away_open_spread = -home_open_spread
            S_H_cl_line = -S_cl_line if home_ml < away_ml else S_cl_line
            S_A_cl_line = -S_H_cl_line
            h2_home_spread = -h2_spread if home_ml < away_ml else h2_spread
            h2_away_spread = -h2_home_spread

            new_df["season"].append(row["season"])
            new_df["date"].append(row["date"])
            new_df["home_team"].append(self._translate(next_row["name"]))
            new_df["away_team"].append(self._translate(row["name"]))
            new_df["home_1stQtr"].append(next_row["1stQtr"])
            new_df["away_1stQtr"].append(row["1stQtr"])
            new_df["home_2ndQtr"].append(next_row["2ndQtr"])
            new_df["away_2ndQtr"].append(row["2ndQtr"])
            new_df["home_3rdQtr"].append(next_row["3rdQtr"])
            new_df["away_3rdQtr"].append(row["3rdQtr"])
            new_df["home_4thQtr"].append(next_row["4thQtr"])
            new_df["away_4thQtr"].append(row["4thQtr"])
            new_df["home_final"].append(next_row["final"])
            new_df["away_final"].append(row["final"])
            new_df["ML_H_cl_odds"].append(home_ml)
            new_df["ML_A_cl_odds"].append(away_ml)
            new_df["home_open_spread"].append(home_open_spread)
            new_df["away_open_spread"].append(away_open_spread)
            new_df["S_H_cl_line"].append(S_H_cl_line)
            new_df["S_A_cl_line"].append(S_A_cl_line)
            new_df["home_2H_spread"].append(h2_home_spread)
            new_df["away_2H_spread"].append(h2_away_spread)
            new_df["2H_total"].append(h2_total)
            new_df["OU_op_line"].append(open_ou)
            new_df["OU_cl_line"].append(close_ou)

        return pd.DataFrame(new_df)


# NBA is the same as NFL, so we can subclass the NFL scraper
class NBAOddsScraper(NFLOddsScraper):
    def __init__(self, years):
        super().__init__(years)
        self.sport = "nba"
        self.base = (
            "https://www.sportsbookreviewsonline.com/scoresoddsarchives/nba-odds-"
        )
        self.schema = {
            "season": [],
            "date": [],
            "home_team": [],
            "away_team": [],
            "home_1stQtr": [],
            "away_1stQtr": [],
            "home_2ndQtr": [],
            "away_2ndQtr": [],
            "home_3rdQtr": [],
            "away_3rdQtr": [],
            "home_4thQtr": [],
            "away_4thQtr": [],
            "home_final": [],
            "away_final": [],
            "ML_H_cl_odds": [],
            "ML_A_cl_odds": [],
            "home_open_spread": [],
            "away_open_spread": [],
            "S_H_cl_line": [],
            "S_A_cl_line": [],
            "home_2H_spread": [],
            "away_2H_spread": [],
            "2H_total": [],
            "OU_op_line": [],
            "OU_cl_line": [],
        }


# NHL is the same as NFL, so we can subclass the NFL scraper
class NHLOddsScraper(OddsScraper):
    def __init__(self, years):
        super().__init__("nhl", years)
        self.base = (
            "https://www.sportsbookreviewsonline.com/scoresoddsarchives/nhl-odds-"
        )
        self.schema = {
            "season": [],
            "date": [],
            "home_team": [],
            "away_team": [],
            "home_1stPeriod": [],
            "away_1stPeriod": [],
            "home_2ndPeriod": [],
            "away_2ndPeriod": [],
            "home_3rdPeriod": [],
            "away_3rdPeriod": [],
            "home_final": [],
            "away_final": [],
            "ML_H_op_odds": [],
            "ML_A_op_odds": [],
            "ML_H_cl_odds": [],
            "ML_A_cl_odds": [],
            "S_H_cl_line": [],
            "S_A_cl_line": [],
            "S_H_cl_odds": [],
            "S_A_cl_odds": [],
            "OU_op_line": [],
            "OU_op_odds": [],
            "OU_cl_line": [],
            "OU_cl_odds": [],
        }

    def _reformat_data(self, df, season, covid=False):
        new_df = pd.DataFrame()
        new_df["season"] = [season] * len(df)
        new_df["date"] = df[0].apply(
            lambda x: (
                self._make_datestr(x, season)
                if not covid
                else self._make_datestr(x, season, start=1, yr_end=3)
            )
        )
        new_df["name"] = df[3]
        new_df["1stPeriod"] = df[4]
        new_df["2ndPeriod"] = df[5]
        new_df["3rdPeriod"] = df[6]
        new_df["final"] = df[7]
        new_df["open_ml"] = df[8]
        new_df["open_ml"] = new_df["open_ml"].apply(
            lambda x: 0 if x in self.blacklist else x
        )
        new_df["close_ml"] = df[9]
        new_df["close_ml"] = new_df["close_ml"].apply(
            lambda x: 0 if x in self.blacklist else x
        )
        new_df["S_cl_line"] = df[10] if season > 2013 else 0
        new_df["S_cl_line"] = new_df["S_cl_line"].apply(
            lambda x: 0 if x in self.blacklist else float(x)
        )
        new_df["S_cl_odds"] = df[11] if season > 2013 else 0
        new_df["S_cl_odds"] = new_df["S_cl_odds"].apply(
            lambda x: 0 if x in self.blacklist else float(x)
        )
        new_df["OU_op_line"] = df[12] if season > 2013 else df[10]
        new_df["OU_op_line"] = new_df["OU_op_line"].apply(
            lambda x: 0 if x in self.blacklist else float(x)
        )
        new_df["OU_op_odds"] = df[13] if season > 2013 else df[11]
        new_df["OU_op_odds"] = new_df["OU_op_odds"].apply(
            lambda x: 0 if x in self.blacklist else float(x)
        )
        new_df["OU_cl_line"] = df[14] if season > 2013 else df[12]
        new_df["OU_cl_line"] = new_df["OU_cl_line"].apply(
            lambda x: 0 if x in self.blacklist else float(x)
        )
        new_df["OU_cl_odds"] = df[15] if season > 2013 else df[13]
        new_df["OU_cl_odds"] = new_df["OU_cl_odds"].apply(
            lambda x: 0 if x in self.blacklist else float(x)
        )

        return new_df

    def _to_schema(self, df):
        new_df = self.schema.copy()
        df = df.fillna(0)
        progress = df.iterrows()
        # remove the first row, as it is the header
        next(progress)
        for (i1, row), (i2, next_row) in self._pairwise(progress):
            # skip every other row
            if i1 % 2 == 0:
              continue

            new_df["season"].append(row["season"])
            new_df["date"].append(row["date"])
            new_df["home_team"].append(self._translate(next_row["name"]))
            new_df["away_team"].append(self._translate(row["name"]))
            new_df["home_1stPeriod"].append(next_row["1stPeriod"])
            new_df["away_1stPeriod"].append(row["1stPeriod"])
            new_df["home_2ndPeriod"].append(next_row["2ndPeriod"])
            new_df["away_2ndPeriod"].append(row["2ndPeriod"])
            new_df["home_3rdPeriod"].append(next_row["3rdPeriod"])
            new_df["away_3rdPeriod"].append(row["3rdPeriod"])
            new_df["home_final"].append(next_row["final"])
            new_df["away_final"].append(row["final"])
            new_df["ML_H_op_odds"].append(int(next_row["open_ml"]))
            new_df["ML_A_op_odds"].append(int(row["open_ml"]))
            new_df["ML_H_cl_odds"].append(int(next_row["close_ml"]))
            new_df["ML_A_cl_odds"].append(int(row["close_ml"]))
            new_df["S_H_cl_line"].append(next_row["S_cl_line"])
            new_df["S_A_cl_line"].append(row["S_cl_line"])
            new_df["S_H_cl_odds"].append(next_row["S_cl_odds"])
            new_df["S_A_cl_odds"].append(row["S_cl_odds"])
            new_df["OU_op_line"].append(next_row["OU_op_line"])
            new_df["OU_op_odds"].append(next_row["OU_op_odds"])
            new_df["OU_cl_line"].append(next_row["OU_cl_line"])
            new_df["OU_cl_odds"].append(next_row["OU_cl_odds"])

        return pd.DataFrame(new_df)

    def driver(self):
        dfs = pd.DataFrame()
        for season in self.seasons:
            # compensate for the COVID shortened season in 2021
            season_str = self._make_season(season) if season != 2020 else "2021"
            is_cov = True if season == 2020 else False
            url = self.base + season_str

            # Sportsbookreview has scraper protection, so we need to set a user agent
            # to get around this.
            headers = {"User-Agent": "Mozilla/5.0"}
            r = requests.get(url, headers=headers)

            dfs = pd.concat(
                [dfs, self._reformat_data(pd.read_html(r.text)[0][1:], season, is_cov)],
                axis=0,
            )

        return self._to_schema(dfs)


# MLB has a different format, so we need to subclass the OddsScraper
class MLBOddsScraper(OddsScraper):
    def __init__(self, years):
        super().__init__("mlb", years)
        self.base = "https://www.sportsbookreviewsonline.com/wp-content/uploads/sportsbookreviewsonline_com_737/mlb-odds-"
        self.ext = ".xlsx"
        self.schema = {
          "season": [],
          "date": [],
          "a_name": [],
          "h_name": [],
          "a_final": [],
          "h_final": [],
          "a_SP": [],
          "a_thr": [],
          "h_SP": [],
          "h_thr": [],
          "a_i1": [],
          "a_i2": [],
          "a_i3": [],
          "a_i4": [],
          "a_i5": [],
          "a_i6": [],
          "a_i7": [],
          "a_i8": [],
          "a_i9": [],
          "h_i1": [],
          "h_i2": [],
          "h_i3": [],
          "h_i4": [],
          "h_i5": [],
          "h_i6": [],
          "h_i7": [],
          "h_i8": [],
          "h_i9": [],
          "a_ML_op": [],
          "h_ML_op": [],
          "a_ML_cl": [],
          "h_ML_cl": [],
          "a_S_cl_line": [],
          "a_S_cl_odds": [],
          "h_S_cl_line": [],
          "h_S_cl_odds": [],
          "OU_op_line": [],
          "O_op_odds": [],
          "U_op_odds": [],
          "OU_cl_line": [],
          "O_cl_odds": [],
          "U_cl_odds": [],
        }

    def _reformat_data(self, df, season):
      new_df = pd.DataFrame()
      new_df["season"] = df[0].apply(lambda x: season)
      new_df["date"] = df[0].apply(lambda x: self._make_datestr(x, season, start=3, yr_end=11))
      new_df["team"] = df[3]
      new_df["pName"] = df[4].apply(lambda x: \
        None if (type(x) != str) \
        else x[:-2] if ("-L" in x or "-R" in x) \
        else x
      )
      new_df["pThrow"] = df[4].apply(lambda x: \
        None if (type(x) != str) \
        else 'L' if ("-L" in x) \
        else 'R'
      )
      new_df["1stInn"] = df[5]
      new_df["2ndInn"] = df[6]
      new_df["3rdInn"] = df[7]
      new_df["4thInn"] = df[8]
      new_df["5thInn"] = df[9]
      new_df["6thInn"] = df[10]
      new_df["7thInn"] = df[11]
      new_df["8thInn"] = df[12]
      new_df["9thInn"] = df[13]
      new_df["final"] = df[14]
      new_df["open_ml"] = df[15]
      new_df["close_ml"] = df[16]
      new_df["S_cl_line"] = df[17] if season > 2013 else 0
      new_df["S_cl_odds"] = df[18] if season > 2013 else 0
      new_df["OU_op_line"] = df[19] if season > 2013 else df[17]
      new_df["OU_op_odds"] = df[20] if season > 2013 else df[18]
      new_df["OU_cl_line"] = df[21] if season > 2013 else df[19]
      new_df["OU_cl_odds"] = df[22] if season > 2013 else df[20]

      return new_df

    def _to_schema(self, df):
      print('processing data for the requested seasons...')
      new_df = self.schema.copy()
      df = df.reset_index(drop=True)
      for i in range(len(df)):
        if i % 2 == 0:
          row = df.loc[i+0]
          next_row = df.loc[i+1]

          away = self._translate(row["team"])
          home = self._translate(next_row["team"])
          new_df["season"].append(row["season"])
          new_df["date"].append(row["date"])
          new_df["a_name"].append(away)
          new_df["h_name"].append(home)
          new_df["a_final"].append(row["final"])
          new_df["h_final"].append(next_row["final"])
          new_df["a_SP"].append(row["pName"])
          new_df["a_thr"].append(row["pThrow"])
          new_df["h_SP"].append(next_row["pName"])
          new_df["h_thr"].append(next_row["pThrow"])
          new_df["a_i1"].append(row["1stInn"])
          new_df["a_i2"].append(row["2ndInn"])
          new_df["a_i3"].append(row["3rdInn"])
          new_df["a_i4"].append(row["4thInn"])
          new_df["a_i5"].append(row["5thInn"])
          new_df["a_i6"].append(row["6thInn"])
          new_df["a_i7"].append(row["7thInn"])
          new_df["a_i8"].append(row["8thInn"])
          new_df["a_i9"].append(row["9thInn"])
          new_df["h_i1"].append(next_row["1stInn"])
          new_df["h_i2"].append(next_row["2ndInn"])
          new_df["h_i3"].append(next_row["3rdInn"])
          new_df["h_i4"].append(next_row["4thInn"])
          new_df["h_i5"].append(next_row["5thInn"])
          new_df["h_i6"].append(next_row["6thInn"])
          new_df["h_i7"].append(next_row["7thInn"])
          new_df["h_i8"].append(next_row["8thInn"])
          new_df["h_i9"].append(next_row["9thInn"])
          new_df["a_ML_op"].append(row["open_ml"])
          new_df["h_ML_op"].append(next_row["open_ml"])
          new_df["a_ML_cl"].append(row["close_ml"])
          new_df["h_ML_cl"].append(next_row["close_ml"])
          new_df["a_S_cl_line"].append(row["S_cl_line"])
          new_df["a_S_cl_odds"].append(row["S_cl_odds"])
          new_df["h_S_cl_line"].append(next_row["S_cl_line"])
          new_df["h_S_cl_odds"].append(next_row["S_cl_odds"])
          new_df["OU_op_line"].append(row["OU_op_line"])
          new_df["O_op_odds"].append(row["OU_op_odds"])
          new_df["U_op_odds"].append(next_row["OU_op_odds"])
          new_df["OU_cl_line"].append(row["OU_cl_line"])
          new_df["O_cl_odds"].append(row["OU_cl_odds"])
          new_df["U_cl_odds"].append(next_row["OU_cl_odds"])

          assert row["date"] == next_row["date"], \
            f'date mismatch; {away}@{home} on {row["date"]} vs. {next_row["date"]}; check row formatting'

          if (not isinstance(row["open_ml"], (int, float)) or not isinstance(next_row["open_ml"], (int, float)) \
              or not isinstance(row["close_ml"], (int, float)) or not isinstance(next_row["close_ml"], (int, float))):
            print(f'WARNING: invalid ML odds found; {away}@{home} on {row["date"]}; Validate this entry manually:')
            # print(pd.DataFrame(new_df).loc[[int(i/2)]]); print('')

          if (row["S_cl_line"] != -1*next_row["S_cl_line"]):
            print(f'WARNING: run line values should be additive inverses; {away}@{home} on {row["date"]}; Validate this entry manually:')
            # print(pd.DataFrame(new_df).loc[[int(i/2)]]); print('')

          if (not isinstance(row["S_cl_odds"], (int, float)) or not isinstance(next_row["S_cl_odds"], (int, float))):
            print(f'WARNING: invalid spread odds found; {away}@{home} on {row["date"]}; Validate this entry manually:')
            # print(pd.DataFrame(new_df).loc[[int(i/2)]]); print('')

          assert row["OU_op_line"] == next_row["OU_op_line"], \
            f'opening line mismatch; {away}@{home} on {row["date"]}; O={row["OU_op_line"]}, U={next_row["OU_op_line"]}'

          assert row["OU_cl_line"] == next_row["OU_cl_line"], \
            f'closing line mismatch; {away}@{home} on {row["date"]}; O={row["OU_cl_line"]}, U={next_row["OU_cl_line"]}'

          if (not isinstance(row["OU_op_odds"], (int, float)) or not isinstance(next_row["OU_op_odds"], (int, float)) \
              or not isinstance(row["OU_cl_odds"], (int, float)) or not isinstance(next_row["OU_cl_odds"], (int, float))):
            print(f'WARNING: invalid OU odds found; {away}@{home} on {row["date"]}; Validate this entry manually:')
            # print(pd.DataFrame(new_df).loc[[int(i/2)]]); print('')

      print('set the following dataframe for the requested seasons:')
      print(pd.DataFrame(new_df))
      return pd.DataFrame(new_df)

    def driver(self):
      dfs = pd.DataFrame()
      if not os.path.isdir('data/src'):
          os.mkdir('data/src')
      for season in self.seasons:
        src_file = 'data/src/mlb-odds-%d.xlsx' % season
        if os.path.isfile(src_file):
          print('found %s, skipping download' % src_file)
        else:
          print('downloading %s' % src_file)
          url = self.base + str(season) + self.ext
          headers = {"User-Agent": "Mozilla/5.0"}
          r = requests.get(url, headers=headers)
          with open(src_file, "wb") as f:
            f.write(r.content)

        df = pd.read_excel(src_file, header=None, sheet_name=None)
        print('opening %s for processing' % src_file)
        dfs = pd.concat(
          [dfs, self._reformat_data(df["Sheet1"][1:], season)], axis=0
        )

      return self._to_schema(dfs)
