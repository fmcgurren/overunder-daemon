import datetime
import requests
import os

from apscheduler.schedulers.blocking import BlockingScheduler

from betfair import BetfairSettings
from betfair import Betfair

# ----------------------------------
# HELPER CLASSES
# ----------------------------------


class StrategySettings:
    def __init__(self, eventLookAheadMinutes, minBackStake, minBackPrice, maxBackPrice, minLayPrice, maxLayPrice, placementThresholdMinutes, targetProfitPercent, stopLossThresholdMinutes, stopLossPercent, overroundThreshold, matchedAmountThreshold, marketsToTrade, excludedTeams):
        self.eventLookAheadMinutes = eventLookAheadMinutes
        self.minBackStake = minBackStake
        self.minBackPrice = minBackPrice
        self.maxBackPrice = maxBackPrice
        self.minLayPrice = minLayPrice
        self.maxLayPrice = maxLayPrice
        self.placementThresholdMinutes = placementThresholdMinutes
        self.targetProfitPercent = targetProfitPercent
        self.stopLossThresholdMinutes = stopLossThresholdMinutes
        self.stopLossPercent = stopLossPercent
        self.overroundThreshold = overroundThreshold
        self.matchedAmountThreshold = matchedAmountThreshold
        self.marketsToTrade = marketsToTrade
        self.excludedTeams = excludedTeams

# ----------------------------------
# STRATEGY
# ----------------------------------


class OverUnderStrategy:
    def __init__(self, strategySettings, betfairSettings):
        self.strategySettings = strategySettings
        self.betfairSettings = betfairSettings

        self.sessionTokenExpiresAt = datetime.datetime.now()
        self.sessionTokenValidForMinutes = 10
        self.backStake = 2.0
        self.betfair = None
        self.tradedMarketIds = []

        # init betfair
        self.refreshSessionToken()
        self.betfair = Betfair(self.betfairSettings)

        # bootstrap tradedMarketIds
        self.bootstrapTradedMarketIds()

# ----------------------------------
# METHODS
# ----------------------------------
    def bootstrapTradedMarketIds(self):

        currentOrders = self.betfair.listCurrentOrders()

        if currentOrders is None:
            return

        # marketIds with unmatched bets
        for order in currentOrders['currentOrders']:
            if order['sizeMatched'] == 0.0:
                if order['marketId'] not in self.tradedMarketIds:
                    self.tradedMarketIds.append(order['marketId'])

    def iteration(self):
        startDate = datetime.datetime.now()
        print('START: %s' % startDate)

        # sessionToken expired?
        if self.sessionTokenExpiresAt < datetime.datetime.now():
            self.refreshSessionToken()

        # init betfair - handles initialiation of headers
        self.betfair = Betfair(self.betfairSettings)

        # account funds, back stake
        accountFunds = self.betfair.getAccountFunds()

        if accountFunds is None:
            return

        availableToBetBalance = accountFunds['availableToBetBalance']
        exposure = accountFunds['exposure']

        # determine backStake
        self.backStake = round(float(availableToBetBalance) * 0.04, 2)

        if self.backStake < self.strategySettings.minBackStake:
            self.backStake = self.strategySettings.minBackStake

        # trade existing positions
        self.tradeExistingMarketPositions()

        # TODO: Insufficient Funds
        if availableToBetBalance < self.backStake:
            print('INSUFFICIENT FUNDS: {}'.format(availableToBetBalance))
            return

        # harvest and process new events
        eventLookAhead = datetime.datetime.now(
        ) + datetime.timedelta(minutes=self.strategySettings.eventLookAheadMinutes)
        eventLookAheadDateTime = eventLookAhead.strftime('%Y-%m-%dT%H:%M:%SZ')
        events = self.betfair.listEvents('1', eventLookAheadDateTime)

        if events is not None:
            self.processEvents(events)

        endDate = datetime.datetime.now()
        delta = endDate - startDate
        print('END:   %s duration: %d secs availableToBetBalance: %s exposure: %s' % (
            endDate, delta.seconds, availableToBetBalance, exposure))

    def processEvents(self, events):

        for event in events:
            eventDetails = event['event']

            # black listed teams
            if any(team in eventDetails['name'] for team in self.strategySettings.excludedTeams):
                return

            # ignore if too far in future
            placementDateTimeThreshold = datetime.datetime.now(
            ) + datetime.timedelta(minutes=self.strategySettings.placementThresholdMinutes)
            eventDateTime = datetime.datetime.strptime(
                eventDetails['openDate'], '%Y-%m-%dT%H:%M:%S.%fZ')

            if eventDateTime > placementDateTimeThreshold:
                continue

            # get market details
            markets = self.betfair.getMarketCatalogueForEvent(
                '1', eventDetails['id'], True)

            # check and establish position
            for market in markets:
                # skip if market is already being traded
                if market['marketId'] in self.tradedMarketIds:
                    continue

                # liquidity check
                if(market['totalMatched'] < matchedAmountThreshold):
                    continue

                # establish new market position if market is eligible for trading
                if str(market['marketName']) in self.strategySettings.marketsToTrade:
                    marketBook = self.betfair.getMarketBookBestOffers(
                        market['marketId'])
                    if marketBook is not None:
                        self.establishMarketPosition(
                            eventDetails, market, marketBook)

    def tradeExistingMarketPositions(self):

        for marketId in self.tradedMarketIds:
            self.tradeMarketPosition(marketId)

    def tradeMarketPosition(self, marketId):
        print('TRADING: %s' % marketId)

        # get current position
        currentOrders = self.betfair.listCurrentOrders(marketId)

        # shortcircuit if API exception
        if currentOrders == None:
            return

        if currentOrders['currentOrders'] == []:
            # print ('currentOrders == Empty')
            self.tradedMarketIds.remove(marketId)
            return

        # if FOK did not match then cancel all orders in the Market
        # attempt will be made to place bet again on next ieration pass
        if len(currentOrders['currentOrders']) == 1 and currentOrders['currentOrders'][0]['side'] == 'LAY':
            print(
                "FOK BACK ORDER DIDNOTMATCH: Canceling and placing again on next iteration.")
            if self.betfair.cancelOrders(marketId):
                print('CEASETRADING: marketId {}'.format(marketId))
                self.tradedMarketIds.remove(marketId)

            return

        filledOrderCount = 0
        # default required so that placedDatetime is not undefined
        placedDatetime = datetime.datetime.now()

        for order in currentOrders['currentOrders']:

            if order['sizeRemaining'] == 0.0:  # original FOK order is fully matched
                placedDatetime = datetime.datetime.strptime(
                    order['placedDate'], '%Y-%m-%dT%H:%M:%S.%fZ')
                filledOrderCount = filledOrderCount + 1
                selectionId = order['selectionId']
                price = order['priceSize']['price']
                size = order['priceSize']['size']
                side = 'BACK' if order['side'] == 'LAY' else 'LAY'

        # execute stop loss if triggered by stopLossThresholdMinutes
        stopLossDatetimeThreshold = placedDatetime + \
            datetime.timedelta(
                minutes=self.strategySettings.stopLossThresholdMinutes)

        # TODO: uncomment if unsure
        # print ("filledOrderCount = {}, {} > {}" .format(filledOrderCount, datetime.datetime.now(), stopLossDatetimeThreshold))

        if filledOrderCount == 1 and datetime.datetime.now() > stopLossDatetimeThreshold:
            # calculate stop loss percent based on time and stepping back 1% with each 10 second iteration
            timedelta = datetime.datetime.now() - stopLossDatetimeThreshold
            # print ("timedelta {}".format(timedelta))

            stepBackProfitPercentModifier = round(
                timedelta.seconds / 10, 0) / 100
            #print ("stepBackProfitPercentModifier {}".format(stepBackProfitPercentModifier))

            revisedProfitPercent = round(
                (1 + self.strategySettings.targetProfitPercent) - stepBackProfitPercentModifier, 2)
            # print ("revisedProfitPercent {}".format(revisedProfitPercent))

            if revisedProfitPercent < 1.0:
                revisedProfitPercent = 0.50

            # hedge order is a lay
            if side == 'LAY':
                total = round(size * price, 2)

                revisedStake = round(size * revisedProfitPercent, 2)

                # revisedStake must obey min stake
                if revisedStake < 2.0:
                    revisedStake = 2.0
                    #print ('MarketId: {} trading has hit min revised Stake and is no longer tradable.'.format(marketId))

                newPrice = self.applyOddsLadder(total / revisedStake)

                print("STOPLOSS: timedelta: {} stepBack: {} revisedProfit: {} newPrice: {} revisedStake: {}".format(
                    timedelta, stepBackProfitPercentModifier, revisedProfitPercent, newPrice, revisedStake))
                # cancel & place new hedge order as per stop loss settings
                if self.betfair.cancelOrders(marketId):
                    if revisedStake == 2.0:
                        self.betfair.placeOrder(
                            marketId, selectionId, side, revisedStake, newPrice)
                        print('MINSTAKE CEASETRADING: marketId {}'.format(marketId))
                        self.tradedMarketIds.remove(marketId)
                    else:
                        #self.betfair.placeOrderByPayout(marketId, selectionId, side, 10.0, total)
                        self.betfair.placeFOKOrder(
                            marketId, selectionId, side, revisedStake, newPrice)

        # remove from traded markets if both orders filled
        if filledOrderCount == 2:
            print('POSITIONCLOSED: marketId: {}'.format(marketId))
            self.tradedMarketIds.remove(marketId)

    def establishMarketPosition(self, eventDetails, market, marketBook):

        marketId = market['marketId']
        currentOrders = self.betfair.listCurrentOrders(marketId)

        if currentOrders is None:
            return

        # shortcircuit if position established elsewhere (e.g. directly on website)
        if currentOrders['currentOrders'] != []:
            return

        # Unders
        undersSelectionId = market['runners'][0]['selectionId']
        underCurrentBackPrice, underCurrentLayPrice = self.betfair.getCurrentBestPrices(
            marketBook, undersSelectionId)

        if underCurrentBackPrice is not None and underCurrentLayPrice is not None:
            if underCurrentBackPrice > self.strategySettings.minBackPrice and underCurrentBackPrice < self.strategySettings.maxBackPrice:
                overround = (underCurrentLayPrice /
                             underCurrentBackPrice) * 100
                if overround < overroundThreshold:
                    print('OPENING POSITION: {} - {} backing selection: {}'.format(
                        eventDetails['name'], market['marketName'], market['runners'][0]['runnerName']))

                    # determine and place order pair (keep in running)
                    '''
                    Back the Under

                    Target Profit (e.g 0.25)
                    Total = Stake * Odds (e.g. 2.0 * 2.6 = 5.2)
                    Hedge Stake = Stake + Target Profit (e.g. 2.0 + 0.25 = 2.25)
                    Hedge Odds = Total / HedgeStake
                    '''
                    stake = self.backStake

                    targetProfit = stake * self.strategySettings.targetProfitPercent
                    total = stake * underCurrentBackPrice
                    hedgeStake = round(self.backStake + targetProfit, 2)
                    hedgeOdds = self.applyOddsLadder(total / hedgeStake)

                    if self.betfair.placeBackTheUnderPair(marketId, undersSelectionId, stake, underCurrentBackPrice, undersSelectionId, hedgeStake, hedgeOdds) == True:
                        self.tradedMarketIds.append(marketId)

# ----------------------------------
# HELPERS
# ----------------------------------
    ''' 
    # Betfair Odds Ladder
    1.01 → 2	0.01
    2→ 3	0.02
    3 → 4	0.05
    4 → 6	0.1
    6 → 10	0.2
    10 → 20	0.5
    20 → 30	1
    30 → 50	2
    50 → 100	5
    100 → 1000	10
    '''

    def applyOddsLadder(self, odds):
        odds = round(odds, 2)
        if odds <= 2.0:
            return odds
        if odds > 2.0 and odds <= 3:
            temp = round(odds / 0.02, 0)
            return round(temp * 0.02, 2)
        if odds > 3.0 and odds <= 4:
            temp = round(odds / 0.05, 0)
            return round(temp * 0.05, 2)
        if odds > 4.0 and odds <= 6:
            temp = round(odds / 0.1, 0)
            return round(temp * 0.1, 2)
        if odds > 6.0 and odds <= 10:
            temp = round(odds / 0.2, 0)
            return round(temp * 0.2, 2)
        if odds > 10.0 and odds <= 20:
            temp = round(odds / 0.5, 0)
            return round(temp * 0.5, 2)
        if odds > 20.0 and odds <= 30:
            temp = round(odds / 1.0, 0)
            return round(temp * 1.0, 2)
        if odds > 30.0 and odds <= 50:
            temp = round(odds / 2.0, 0)
            return round(temp * 2.0, 2)
        if odds > 50.0 and odds <= 100:
            temp = round(odds / 5.0, 0)
            return round(temp * 5.0, 2)
        if odds > 100.0 and odds <= 1000:
            temp = round(odds / 10.0, 0)
            return round(temp * 10.0, 2)

    def refreshSessionToken(self):
        print('SESSION TOKEN REFRESH...')

        username = os.environ.get("BETFAIR_USERNAME")
        password = os.environ.get("BETFAIR_PASSWORD")
        credentials = 'username=%s&password=%s' % (username, password)

        payload = credentials
        headers = {'X-Application': 'daemon',
                   'Content-Type': 'application/x-www-form-urlencoded'}

        # OLD https://identitysso.betfair.com/api/certlogin
        resp = requests.post('https://identitysso-cert.betfair.com/api/certlogin',
                             data=payload, cert=('client-2048.crt', 'client-2048.key'), headers=headers)

        if resp.status_code == 200:
            resp_json = resp.json()
            print(resp_json['loginStatus'])
            print(resp_json['sessionToken'])
            self.sessionToken = resp_json['sessionToken']
            self.sessionTokenExpiresAt = self.sessionTokenExpiresAt + \
                datetime.timedelta(minutes=self.sessionTokenValidForMinutes)
            self.betfairSettings.sessionToken = self.sessionToken
            self.betfairSettings.updateHeaders()
        else:
            print("Login Request failed.")
            self.sessionToken = None


# ----------------------------------
# MAIN
# ----------------------------------
if __name__ == '__main__':

    print('### Under Over Strategy: 1 ###')
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    # betfairSettings
    appKey = os.environ.get("BETFAIR_LIVE_KEY")
    sessionToken = None
    bettingURL = "https://api.betfair.com/exchange/betting/json-rpc/v1"
    accountsURL = "https://api.betfair.com/exchange/account/json-rpc/v1"

    betfairSettings = BetfairSettings(
        appKey, sessionToken, bettingURL, accountsURL)

    # strategySettings
    eventLookAheadMinutes = 10
    minBackStake = 2.0  # 3.4
    minBackPrice = 1.6  # 2.0
    maxBackPrice = 2.8  # 2.8
    minLayPrice = 1.9
    maxLayPrice = 2.2
    placementThresholdMinutes = 2
    targetProfitPercent = 0.16  # = 0.28 with 2.0 stake
    stopLossPercent = 0.4  # = 0.80 with 2.0 stake
    stopLossThresholdMinutes = 16
    overroundThreshold = 105
    matchedAmountThreshold = 1000
    marketsToTrade = ['Over/Under 2.5 Goals']
    excludedTeams = ['Man City', 'Fulham', 'Newcastle',  # Premiership
                     'Bolton', 'Derby', 'Aston Villa', 'Reading', 'Nottingham Forest', 'Sheff Utd', 'Brentford',  # Championship
                     'Charlton', 'Plymouth', 'Scunthorpe', 'Wycombe',  # League One
                     'Swindon', 'Newport', 'Colchester',  # League Two
                     'Bromley', 'Maidenhead',  # League
                     'Celtic', 'Rangers', 'Dundee',  # SPL


                     'Sarpsborg',  # ?
                     'Bercelona', 'Levante', 'Sevilla',  # La Liga
                     'Mallorca', 'Oviedo', 'Extremadura UD', 'Tenerife', 'Zaragoza', 'Las Palmas', 'Cordoba', 'Alcorcon',  # Spain 2?
                     'Twente', 'PSV', 'Ajax', 'Heracles', 'Az Alkmaar', 'Willem II', 'Emmen',  # Eredivisie
                     'Legia Warsaw', 'Zaglebie Lubin', 'Miedz Legnica',  # Poland 1
                     'Eupen', 'Kortrijk', 'Club Brugge',  # Begium A
                     'Tubize', 'Yellow-Red Mechelen',  # Begium B
                     'Krupa', 'Zrinjski Mostar',  # Bosnia
                     'Vitosha Bistrica', 'Cherno More Varna',  # Bulgaria
                     'Antofagasta', 'Espanola',  # Chile
                     'America Cali',  # Columbia
                     'Inter Zapresic', 'Rijeka', 'NK Istra',  # Croatia
                     'Slavia Prague', 'Dukla Prague',  # Czech Republic
                     'Apollon Limassol',  # Cyprus
                     'Zenit St Petersburg', 'CSKA Moscow',  # Russia 1
                     'Botosani', 'Dinamo Bucharest',  # Romania 1
                     'Hartberg', 'FC Wacker Innsbruck',  # Austria 1
                     'Brisbane Roar',  # Australia
                     'Sabah',  # Azerbaijan
                     'AC Horsens', 'FC Nordsjaelland', 'FC Copenhagen',  # Denmark
                     'Kalju', 'Parnu JK Vaprus', 'Viljandi Tulevik',  # Estonia
                     'Zamalec', 'Al Ittihad',  # Ejypt
                     'Alianza FC (SLV)', 'Firpo',  # El Salvador
                     'Paris St-G', 'Rennes', 'Nantes', 'Guingamp', 'Lille',  # France 1
                     'Valenciennes', 'Auxerre', 'Lens',  # France 2
                     'RB Leipzig', 'Mgladbach', 'Bayern Munich', 'Schalke',  # Germany A
                     'Hamburg', 'Jahn Regensburg',  # Germany B
                     'Lions', 'FC Boca Juniors',  # Gibraltar
                     'Apollon Smirnis',  # Greece
                     'Chennaiyin FC', 'Chennaiyin', 'FC Goa', 'Goa', 'FC Pune City', 'FC Pune', 'Pune', 'Kerala Blasters', 'Northeast United', 'Northeast United FC', 'Chennaiyin FC', 'Chennaiyin', 'ATK',  # India

                     'Brecia', 'Lecce', 'Spezia', 'Foggia', 'Ascoli',  # Italy B
                     'Al Sadd (QAT)',
                     'Hapoel Beer Sheva',  # Israel
                     'Garcilaso',  # Peru
                     'Deportivo Saprissa', 'LD Alajuelense', 'Alajuelense', 'UCR', 'CF Universidad de Costa Rica', 'UCR',  # Costa Rica
                     'Atalanta', 'Napoli', 'Sassuolo', 'Empoli', 'Cagliari', 'Frosinone', 'Genoa'  # Italy Seria A
                     ]

    strategySettings = StrategySettings(eventLookAheadMinutes, minBackStake, minBackPrice, maxBackPrice, minLayPrice, maxLayPrice, placementThresholdMinutes,
                                        targetProfitPercent, stopLossThresholdMinutes, stopLossPercent, overroundThreshold, matchedAmountThreshold, marketsToTrade, excludedTeams)

    # create and start
    overUnderStrategy = OverUnderStrategy(strategySettings, betfairSettings)
    overUnderStrategy.iteration()

    scheduler = BlockingScheduler()
    scheduler.add_job(overUnderStrategy.iteration, 'interval', seconds=10)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
