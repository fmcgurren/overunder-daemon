import re

from datetime import datetime, timedelta


class BetMapping:
    def __init__(self, infogoleBet):
        self.infogolBet = infogoleBet
        if infogoleBet['HomeTeamDisplay'] == '':
            infogoleBet['HomeTeamDisplay'] = infogoleBet['HomeTeam']

        if infogoleBet['AwayTeamDisplay'] == '':
            infogoleBet['AwayTeamDisplay'] = infogoleBet['AwayTeam']

        self.eventName = str(
            infogoleBet['HomeTeamDisplay'] + ' v ' + infogoleBet['AwayTeamDisplay'])
        lookAheadDateTime = datetime.strptime(
            infogoleBet['MatchDateTime'], '%Y-%m-%dT%H:%M:%S')
        lookAheadDateTime = lookAheadDateTime + \
            timedelta(hours=3)  # accounting assumably for UTC diff?
        self.eventDateTime = lookAheadDateTime.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.marketName, self.selectionName = self.map().split(',')
        self.marketId = None
        self.selectionId = None
        self.currentBackPrice = None
        self.currentLayPrice = None

    def PrintYourself(self):
        print('eventName: %s' % self.eventName)
        print('verdict: %s' % self.infogolBet['VerdictText'])
        # if self.marketName is not None:
        print('marketName: %s' %
              '<unmapped>' if self.marketName is None else str(self.marketName))
        # if self.selectionName is not None:
        print('selectionName: %s' %
              '<unmapped>' if self.selectionName is None else str(self.selectionName))
        # if self.marketId is not None:
        print('marketId: %s' %
              '<unmapped>' if self.marketId is None else str(self.marketId))
        # if self.selectionId is not None:
        print('selectionId: %s' %
              '<unmapped>' if self.selectionId is None else str(self.selectionId))
        print('currentBackPrice: %s' %
              '<unmapped>' if self.currentBackPrice is None else str(self.currentBackPrice))
        print('currentLayPrice: %s' % '<unmapped>' if self.currentLayPrice is None else str(
            self.currentLayPrice), end='\n\n')

    def map(self):
        if re.match('Both Teams To Score - No', self.infogolBet['VerdictText']) is not None:
            return 'Both teams to Score?,No'
        if re.match('Both Teams To Score', self.infogolBet['VerdictText']) is not None:
            return 'Both teams to Score?,Yes'
        if re.match('%s or Draw' % self.infogolBet['HomeTeamDisplay'], self.infogolBet['VerdictText']) is not None:
            return 'Double Chance,Home or Draw'
        if re.match('%s or Draw' % self.infogolBet['AwayTeamDisplay'], self.infogolBet['VerdictText']) is not None:
            return 'Double Chance,Draw or Away'
        if re.match('%s To Win' % self.infogolBet['HomeTeamDisplay'], self.infogolBet['VerdictText']) is not None:
            return 'Match Odds,%s' % self.infogolBet['HomeTeamDisplay']
        if re.match('%s To Win' % self.infogolBet['AwayTeamDisplay'], self.infogolBet['VerdictText']) is not None:
            return 'Match Odds,%s' % self.infogolBet['AwayTeamDisplay']
        if re.match('Under', self.infogolBet['VerdictText']) is not None:
            return 'Over/{},{}'.format(self.infogolBet['VerdictText'], self.infogolBet['VerdictText'])
        if re.match('Over', self.infogolBet['VerdictText']) is not None:
            return 'Over/Under {},{}'.format(self.infogolBet['VerdictText'][5:], self.infogolBet['VerdictText'])

        return None
