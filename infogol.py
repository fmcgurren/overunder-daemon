import requests
import json
import re

from datetime import datetime, timedelta


class Infogol:

    def callGetBestBets(self, startDate, minConfidence):
        endDate = startDate + timedelta(days=1)

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://www.infogol.net/',
            'Origin': 'https://www.infogol.net',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        params = (
            ('r', 'getBestBets'),
            ('v', [startDate.strftime("%Y-%m-%d"), '-60', '1']),
        )

        # TODO: update request to match modified URL
        # https: // www.infogolapp.com/DataRequest/ExecuteRequest?r = getBestBetsOnDate & v = 2019-03-02 & v = 0 & v = 1

        data = {
            'filterJson': '["AND",[["MatchDateTime","ge","%s"],["MatchDateTime","lt","%s"],["MatchStatus","eq","PreMatch"],["LanguageID","eq",1]]]' % (startDate.strftime("%Y-%m-%dT00:00:00"), endDate.strftime("%Y-%m-%dT00:00:00")),
            'objectName': 'vw_BestBets'
        }

        response = requests.post('https://www.infogolapp.com/DataRequest/ExecuteRequest',
                                 headers=headers, params=params, data=data)

        matches = json.loads(response.text)

        print('*** Match Day: ' + str(startDate.strftime("%Y-%m-%d") + ' ***'))

        filteredBets = list()

        for match in matches:
            if match['VerdictConfidence'] >= minConfidence:
                print(match['HomeTeam'], 'v', match['AwayTeam'], 'Verdict:',
                      match['VerdictText'], 'Confidence', match['VerdictConfidence'])
                filteredBets.append(match)

        return filteredBets
