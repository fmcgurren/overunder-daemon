import urllib
import urllib.request
import urllib.error
import json
import datetime
import sys
import uuid
import datetime
import fuzzywuzzy

from fuzzywuzzy import fuzz
from fuzzywuzzy import process


class BetfairSettings:
    def __init__(self, appKey, sessionToken, bettingURL, accountsURL):
        self.appKey = appKey
        self.sessionToken = sessionToken
        self.bettingURL = bettingURL
        self.accountsURL = accountsURL
        self.headers = {'X-Application': appKey, 'X-Authentication': sessionToken,
                        'content-type': 'application/json'}

    def updateHeaders(self):
        self.headers = {'X-Application': self.appKey, 'X-Authentication': self.sessionToken,
                        'content-type': 'application/json'}

    def PrintYourself(self):
        print('-- BetfairSettings --')
        print('appKey: %s' % self.appKey)
        print('sessionToken: %s' % self.sessionToken)
        print('bettingURL: %s' % self.bettingURL)
        print('accountsURL: %s' % self.accountsURL)
        print('headers: %s' % self.headers)


class Betfair:
    def __init__(self, settings):
        self.settings = settings

    def map(self, betMappings):
        for betMapping in betMappings:
            # try with event name
            matchMarketCatalogue = self.getMarketCatalogueForMatch(
                '1', betMapping.eventDateTime,  betMapping.eventName)
            market = self.getMarket(
                matchMarketCatalogue, betMapping.marketName)

            """ Mapping doesn't always work due to Champions League and U19 in close proximity time wise etc."""

            # try with home team
            if market is None:
                matchMarketCatalogue = self.getMarketCatalogueForMatch(
                    '1', betMapping.eventDateTime,  betMapping.infogolBet['HomeTeamDisplay'])

                # fuzzy match
                matchOdds = self.getMarket(matchMarketCatalogue, 'Match Odds')

                if matchOdds is not None:
                    homeTeam = self.getSelection(
                        matchOdds, betMapping.infogolBet['HomeTeamDisplay'], 65)
                    awayTeam = self.getSelection(
                        matchOdds, betMapping.infogolBet['AwayTeamDisplay'], 65)
                    if(homeTeam is not None and awayTeam is not None):
                        market = self.getMarket(
                            matchMarketCatalogue, betMapping.marketName)

            # try with away team
            if market is None:
                matchMarketCatalogue = self.getMarketCatalogueForMatch(
                    '1', betMapping.eventDateTime,  betMapping.infogolBet['AwayTeamDisplay'])

                # fuzzy match
                matchOdds = self.getMarket(matchMarketCatalogue, 'Match Odds')

                if matchOdds is not None:
                    homeTeam = self.getSelection(
                        matchOdds, betMapping.infogolBet['HomeTeamDisplay'], 65)
                    awayTeam = self.getSelection(
                        matchOdds, betMapping.infogolBet['AwayTeamDisplay'], 65)
                    if(homeTeam is not None and awayTeam is not None):
                        market = self.getMarket(
                            matchMarketCatalogue, betMapping.marketName)

            if market is not None:
                betMapping.marketId = market['marketId']
                selection = self.getSelection(market, betMapping.selectionName)
                if selection is not None:
                    betMapping.selectionId = selection['selectionId']

            if betMapping.selectionId is not None:
                market_book_result = self.getMarketBookBestOffers(
                    betMapping.marketId)
                betMapping.currentBackPrice, betMapping.currentLayPrice = self.getCurrentBestPrices(
                    market_book_result, betMapping.selectionId)
                betMapping.currentLayPrice = self.getCurrentLayPrice(
                    market_book_result, betMapping.selectionId)

            # betMapping.PrintYourself()

        return betMappings

    def callBettingAping(self, jsonrpc_req):
        try:
            req = urllib.request.Request(
                self.settings.bettingURL, jsonrpc_req.encode('utf-8'), self.settings.headers)
            response = urllib.request.urlopen(req)
            jsonResponse = response.read()
            return jsonResponse.decode('utf-8')
        except urllib.error.HTTPError:
            print('Not a valid operation from the service ' +
                  str(self.settings.bettingURL))
            # exit()
            return None
        except urllib.error.URLError as e:
            print(e.reason)
            print('No service available at ' + str(self.settings.bettingURL))
            # exit()
            return None

    def callAccountAping(self, jsonrpc_req):
        try:
            req = urllib.request.Request(
                self.settings.accountsURL, jsonrpc_req.encode('utf-8'), self.settings.headers)
            response = urllib.request.urlopen(req)
            jsonResponse = response.read()
            return jsonResponse.decode('utf-8')
        except urllib.error.HTTPError:
            print('Not a valid operation from the service ' +
                  str(self.settings.accountsURL))
            # exit()
            return None
        except urllib.error.URLError as e:
            print(e.reason)
            print('No service available at ' + str(self.settings.accountsURL))
            # exit()
            return None

    """
    calling getEventTypes operation
    """

    def getEventTypes(self):
        event_type_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listEventTypes", "params": {"filter":{ }}, "id": 1}'
        #print('Calling listEventTypes to get event Type ID')
        eventTypesResponse = self.callBettingAping(event_type_req)
        eventTypeLoads = json.loads(eventTypesResponse)

        try:
            eventTypeResults = eventTypeLoads['result']
            return eventTypeResults
        except:
            print('Exception from API-NG' + str(eventTypeLoads['error']))
            # exit()
            return None

    """
    Extraction eventypeId for eventTypeName from evetypeResults
    """

    def getEventTypeIDForEventTypeName(self, eventTypesResult, requestedEventTypeName):
        if(eventTypesResult is not None):
            for event in eventTypesResult:
                eventTypeName = event['eventType']['name']
                if(eventTypeName == requestedEventTypeName):
                    return event['eventType']['id']
        else:
            print('There is an issue with the input')
            # exit()
            return None

    def getMarketId(self, marketCatalogueResult):
        if(marketCatalogueResult is not None):
            for market in marketCatalogueResult:
                return market['marketId']

    def getMarket(self, marketCatalogueResult, marketName):
        if(marketCatalogueResult is not None):
            for market in marketCatalogueResult:
                if market['marketName'] == marketName:
                    return market

    def getSelectionId(self, marketCatalogueResult):
        if(marketCatalogueResult is not None):
            for market in marketCatalogueResult:
                return market['runners'][0]['selectionId']

    def getSelection(self, market, selectionName, confidenceThreshold=100):
        if(market is not None):
            for selection in market['runners']:
                confidence = fuzz.ratio(selectionName, selection['runnerName'])
                if confidence >= confidenceThreshold:
                    return selection

    def getMarketBookBestOffers(self, marketId):
        #print('Calling listMarketBook to read prices for the Market with ID :' + marketId)
        market_book_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listMarketBook", "params": {"marketIds":["' + \
            marketId + \
            '"],"priceProjection":{"priceData":["EX_BEST_OFFERS"]}}, "id": 1}'
        try:
            """
            print(market_book_req)
            """
            market_book_response = self.callBettingAping(market_book_req)
            """
            print(market_book_response)
            """
            market_book_loads = json.loads(market_book_response)

            market_book_result = market_book_loads['result']
            return market_book_result
        except:
            print('Exception from API-NG' + str(market_book_result['error']))
            # exit()
            return None

    def getCurrentBestPrices(self, market_book_result, selectionId):
        if(market_book_result is not None):
            for marketBook in market_book_result:
                runners = marketBook['runners']
                for runner in runners:
                    if (runner['selectionId'] == selectionId):
                        if (runner['status'] == 'ACTIVE'):
                            try:
                                return runner['ex']['availableToBack'][0]['price'], runner['ex']['availableToLay'][0]['price']
                            except:
                                return None, None
        return None, None

    def getCurrentLayPrice(self, market_book_result, selectionId):
        if(market_book_result is not None):
            for marketBook in market_book_result:
                runners = marketBook['runners']
                for runner in runners:
                    if (runner['selectionId'] == selectionId):
                        if (runner['status'] == 'ACTIVE'):
                            try:
                                return runner['ex']['availableToLay'][0]['price']
                            except:
                                return None

    def printPriceInfo(self, market_book_result):
        if(market_book_result is not None):
            print('Please find Best three available prices for the runners')
            for marketBook in market_book_result:
                runners = marketBook['runners']
                for runner in runners:
                    print('Selection id is ' + str(runner['selectionId']))
                    if (runner['status'] == 'ACTIVE'):
                        print('Available to back price :' +
                              str(runner['ex']['availableToBack'][0]))
                        print('Available to lay price :' +
                              str(runner['ex']['availableToLay']))
                    else:
                        print('This runner is not active')

    def placeBet(self, marketId, selectionId, stake, price):
        if(marketId is not None and selectionId is not None and price is not None):
            customerRef = str(uuid.uuid4().hex)
            print('Calling placeOrder for marketId :' + marketId +
                  ' with selection id :' + str(selectionId) +
                  ' with customerRef :' + customerRef)
            place_order_Req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/placeOrders", "params": {"marketId":"' + str(marketId) + '","instructions":'\
                '[{"selectionId":"' + str(selectionId) + '","handicap":"0","side":"BACK","orderType":"LIMIT","limitOrder":{"size":"' + str(
                    stake) + '","price":"' + str(price) + '","persistenceType":"LAPSE"}}],"customerRef":"' + customerRef + '"}, "id": 1}'
            """
            print(place_order_Req)
            """
            place_order_Response = self.callBettingAping(place_order_Req)
            place_order_load = json.loads(place_order_Response)
            try:
                # uncomment
                place_order_result = place_order_load['result']
                print('Place order status is ' + place_order_result['status'])

                if place_order_result['status'] != 'SUCCESS':
                    print(place_order_result, end='\n\n')

                """
                print('Place order error status is ' + place_order_result['errorCode'])
                print('Reason for Place order failure is ' +
                    place_order_result['instructionReports'][0]['errorCode'])
                """
            except:
                print('Exception from API-NG' +
                      str(place_order_result['error']))
                """
                print(place_order_Response)
                """

    def placeOrderPair(self, marketId, backSelectionId, backStake, backPrice, laySelectionId, layStake, layPrice):

        try:
            customerRef = str(uuid.uuid4().hex)
            print('Calling placeOrder for marketId :' + marketId +
                  ' with back selection id :' + str(backSelectionId) +
                  ' with customerRef :' + customerRef)

            place_order_Req = '{"jsonrpc":"2.0","method":"SportsAPING/v1.0/placeOrders","params":{"marketId":"%s","instructions":[{"selectionId":"%s","handicap":"0","side":"BACK","orderType":"LIMIT","limitOrder":{"size":"%s","price":"%s","persistenceType":"LAPSE"}},{"selectionId":"%s","handicap":"0","side":"LAY","orderType":"LIMIT","limitOrder":{"size":"%s","price":"%s","persistenceType":"PERSIST"}}], "customerRef":"%s"},"id":1}' % (
                str(marketId), str(backSelectionId), str(backStake), str(backPrice), str(laySelectionId), str(layStake), str(layPrice), customerRef)

            print(place_order_Req)

            place_order_Response = self.callBettingAping(place_order_Req)
            #place_order_Response = None

            place_order_load = json.loads(place_order_Response)

            # uncomment
            place_order_result = place_order_load['result']
            print('Place order status is ' + place_order_result['status'])

            if place_order_result['status'] != 'SUCCESS':
                print(place_order_result, end='\n\n')
                return False

            """
            print('Place order error status is ' + place_order_result['errorCode'])
            print('Reason for Place order failure is ' +
                place_order_result['instructionReports'][0]['errorCode'])
            """
        except:
            print('Exception from API-NG' + str(place_order_result['error']))
            """
            print(place_order_Response)
            """
            return False

        return True

    def placeBackTheUnderPair(self, marketId, backSelectionId, backStake, backPrice, laySelectionId, layStake, layPrice):

        try:
            customerRef = str(uuid.uuid4().hex)
            print('Calling placeOrder for marketId :' + marketId +
                  ' with back selection id :' + str(backSelectionId) +
                  ' with customerRef :' + customerRef)

            place_order_Req = '{"jsonrpc":"2.0","method":"SportsAPING/v1.0/placeOrders","params":{"marketId":"%s","instructions":[{"selectionId":"%s","handicap":"0","side":"BACK","orderType":"LIMIT","limitOrder":{"size":"%s","price":"%s","persistenceType":"LAPSE","timeInForce":"FILL_OR_KILL"}},{"selectionId":"%s","handicap":"0","side":"LAY","orderType":"LIMIT","limitOrder":{"size":"%s","price":"%s","persistenceType":"PERSIST"}}], "customerRef":"%s"},"id":1}' % (
                str(marketId), str(backSelectionId), str(backStake), str(backPrice), str(laySelectionId), str(layStake), str(layPrice), customerRef)

            print(place_order_Req)

            place_order_Response = self.callBettingAping(place_order_Req)
            #place_order_Response = None

            place_order_load = json.loads(place_order_Response)

            # uncomment
            place_order_result = place_order_load['result']
            print('Place order status is ' + place_order_result['status'])

            if place_order_result['status'] != 'SUCCESS':
                print(place_order_result, end='\n\n')
                return False

            """
            print('Place order error status is ' + place_order_result['errorCode'])
            print('Reason for Place order failure is ' +
                place_order_result['instructionReports'][0]['errorCode'])
            """
        except:
            print('Exception from API-NG' + str(place_order_result['error']))
            """
            print(place_order_Response)
            """
            return False

        return True

    def placeLayTheOverPair(self, marketId, laySelectionId, layStake, layPrice, backSelectionId, backStake, backPrice):

        try:
            customerRef = str(uuid.uuid4().hex)
            print('Calling placeOrder for marketId :' + marketId +
                  ' with lay selection id :' + str(backSelectionId) +
                  ' with customerRef :' + customerRef)

            place_order_Req = '{"jsonrpc":"2.0","method":"SportsAPING/v1.0/placeOrders","params":{"marketId":"%s","instructions":[{"selectionId":"%s","handicap":"0","side":"LAY","orderType":"LIMIT","limitOrder":{"size":"%s","price":"%s","persistenceType":"LAPSE","timeInForce":"FILL_OR_KILL"}},{"selectionId":"%s","handicap":"0","side":"BACK","orderType":"LIMIT","limitOrder":{"size":"%s","price":"%s","persistenceType":"PERSIST"}}], "customerRef":"%s"},"id":1}' % (
                str(marketId), str(laySelectionId), str(layStake), str(layPrice), str(backSelectionId), str(backStake), str(backPrice), customerRef)

            print(place_order_Req)

            place_order_Response = self.callBettingAping(place_order_Req)
            #place_order_Response = None

            place_order_load = json.loads(place_order_Response)

            # uncomment
            place_order_result = place_order_load['result']
            print('Place order status is ' + place_order_result['status'])

            if place_order_result['status'] != 'SUCCESS':
                print(place_order_result, end='\n\n')
                return False

            """
            print('Place order error status is ' + place_order_result['errorCode'])
            print('Reason for Place order failure is ' +
                place_order_result['instructionReports'][0]['errorCode'])
            """
        except:
            print('Exception from API-NG' + str(place_order_result['error']))
            """
            print(place_order_Response)
            """
            return False

        return True

    def placeOrder(self, marketId, selectionId, side, stake, price):

        try:
            customerRef = str(uuid.uuid4().hex)
            print('Calling placeOrder for marketId :' + marketId +
                  ' with selection id :' + str(selectionId) +
                  ' side :' + str(side) +
                  ' with customerRef :' + customerRef)

            place_order_Req = '{"jsonrpc":"2.0","method":"SportsAPING/v1.0/placeOrders","params":{"marketId":"%s","instructions":[{"selectionId":"%s","handicap":"0","side":"%s","orderType":"LIMIT","limitOrder":{"size":"%s","price":"%s","persistenceType":"PERSIST"}}], "customerRef":"%s"},"id":1}' % (
                str(marketId), str(selectionId), side, str(stake), str(price), customerRef)

            print(place_order_Req)

            place_order_Response = self.callBettingAping(place_order_Req)
            #place_order_Response = None

            place_order_load = json.loads(place_order_Response)

            # uncomment
            place_order_result = place_order_load['result']
            print('Place order status is ' + place_order_result['status'])

            if place_order_result['status'] != 'SUCCESS':
                print(place_order_result, end='\n\n')
                return False

            """
            print('Place order error status is ' + place_order_result['errorCode'])
            print('Reason for Place order failure is ' +
                place_order_result['instructionReports'][0]['errorCode'])
            """
        except:
            print('Exception from API-NG' + str(place_order_result['error']))
            """
            print(place_order_Response)
            """
            return False

        return True

    def placeFOKOrder(self, marketId, selectionId, side, stake, price):

        try:
            customerRef = str(uuid.uuid4().hex)
            print('Calling placeOrder for marketId :' + marketId +
                  ' with selection id :' + str(selectionId) +
                  ' side :' + str(side) +
                  ' with customerRef :' + customerRef)

            place_order_Req = '{"jsonrpc":"2.0","method":"SportsAPING/v1.0/placeOrders","params":{"marketId":"%s","instructions":[{"selectionId":"%s","handicap":"0","side":"%s","orderType":"LIMIT","limitOrder":{"size":"%s","price":"%s","persistenceType":"LAPSE","timeInForce":"FILL_OR_KILL"}}], "customerRef":"%s"},"id":1}' % (
                str(marketId), str(selectionId), side, str(stake), str(price), customerRef)

            print(place_order_Req)

            place_order_Response = self.callBettingAping(place_order_Req)
            #place_order_Response = None

            place_order_load = json.loads(place_order_Response)

            # uncomment
            place_order_result = place_order_load['result']
            print('Place order status is ' + place_order_result['status'])

            if place_order_result['status'] != 'SUCCESS':
                print(place_order_result, end='\n\n')
                return False

            """
            print('Place order error status is ' + place_order_result['errorCode'])
            print('Reason for Place order failure is ' +
                place_order_result['instructionReports'][0]['errorCode'])
            """
        except:
            print('Exception from API-NG' + str(place_order_result['error']))
            """
            print(place_order_Response)
            """
            return False

        return True

    # self.betfair.placeOrderByPayout(marketId, selectionId, side, targetPayout)
    def placeOrderByPayout(self, marketId, selectionId, side, price, targetPayout):

        try:
            customerRef = str(uuid.uuid4().hex)
            print('Calling placeOrderByPayout for marketId :' + marketId +
                  ' with selection id :' + str(selectionId) +
                  ' side :' + str(side) +
                  ' with customerRef :' + customerRef)

            place_order_Req = '{"jsonrpc":"2.0","method":"SportsAPING/v1.0/placeOrders","params":{"marketId":"%s","instructions":[{"selectionId":"%s","handicap":"0","side":"%s","orderType":"LIMIT","limitOrder":{"price":"%s","betTargetType":"PAYOUT","betTargetSize":"%s"}}], "customerRef":"%s"},"id":1}' % (
                str(marketId), str(selectionId), side, str(price), str(targetPayout), customerRef)

            print(place_order_Req)

            place_order_Response = self.callBettingAping(place_order_Req)
            #place_order_Response = None

            place_order_load = json.loads(place_order_Response)

            # uncomment
            place_order_result = place_order_load['result']
            print('Place order status is ' + place_order_result['status'])

            if place_order_result['status'] != 'SUCCESS':
                print(place_order_result, end='\n\n')
                return False

            """
            print('Place order error status is ' + place_order_result['errorCode'])
            print('Reason for Place order failure is ' +
                place_order_result['instructionReports'][0]['errorCode'])
            """
        except:
            print('Exception from API-NG' + str(place_order_result['error']))
            """
            print(place_order_Response)
            """
            return False

        return True

    def cancelOrders(self, marketId):

        try:
            customerRef = str(uuid.uuid4().hex)
            print('cancelaceOrders for marketId :' + marketId +
                  ' with customerRef :' + customerRef)

            cancel_order_Req = '{"jsonrpc":"2.0","method":"SportsAPING/v1.0/cancelOrders","params":{"marketId":"%s", "customerRef":"%s"},"id":1}' % (
                str(marketId), customerRef)

            print(cancel_order_Req)

            cancel_order_Response = self.callBettingAping(cancel_order_Req)
            #place_order_Response = None

            cancel_order_load = json.loads(cancel_order_Response)

            # uncomment
            cancel_order_result = cancel_order_load['result']
            print('Cancel order status is ' + cancel_order_result['status'])

            if cancel_order_result['status'] != 'SUCCESS':
                print(cancel_order_result, end='\n\n')
                return False

            """
            print('Place order error status is ' + place_order_result['errorCode'])
            print('Reason for Place order failure is ' +
                place_order_result['instructionReports'][0]['errorCode'])
            """
        except:
            print('Exception from API-NG' + str(cancel_order_result['error']))
            """
            print(place_order_Response)
            """
            return False

        return True

    def replaceOrder(self, marketId, betId, newPrice):
        customerRef = str(uuid.uuid4().hex)
        print('Calling replaceOrder for betId :' + betId +
              ' on marketId : ' + marketId +
              ' with newPrice :' + str(newPrice) +
              ' customerRef : ' + customerRef)

        replace_order_Req = '{"jsonrpc":"2.0","method":"SportsAPING/v1.0/replaceOrders","params":{"marketId":"%s","instructions":[{"betId":"%s","newPrice":"%s"}], "customerRef":"%s"},"id":1}' % (
            str(marketId), str(betId), str(newPrice), customerRef)

        print(replace_order_Req)

        replace_order_Response = self.callBettingAping(replace_order_Req)
        #place_order_Response = None

        replace_order_load = json.loads(replace_order_Response)
        try:
            # uncomment
            replace_order_result = replace_order_load['result']
            print('RePlace order status is ' + replace_order_result['status'])

            if replace_order_result['status'] != 'SUCCESS':
                print(replace_order_result, end='\n\n')

            """
            print('Place order error status is ' + place_order_result['errorCode'])
            print('Reason for Place order failure is ' +
                place_order_result['instructionReports'][0]['errorCode'])
            """
        except:
            print('Exception from API-NG' + str(replace_order_result['error']))
            """
            print(place_order_Response)
            """

    def getAccountFunds(self):
        #[{"jsonrpc": "2.0", "method": "AccountAPING/v1.0/getAccountFunds", "params": {"wallet":"UK"}, "id": 1}]
        try:
            account_funds_req = '{"jsonrpc": "2.0", "method": "AccountAPING/v1.0/getAccountFunds", "params": {"wallet":"UK"}, "id": 1}'
            """
            print(account_funds_req)
            """
            account_funds_response = self.callAccountAping(account_funds_req)
            """
            print(account_funds_response)
            """
            account_funds_loads = json.loads(account_funds_response)

            account_funds_result = account_funds_loads['result']

            return account_funds_result
        except:
            print('Exception from Account API-NG' +
                  str(account_funds_result['error']))
            # exit()
            return None

    def getMarketCatalogueForMatch(self, eventTypeID, eventDateTime, filter):
        if (eventTypeID is not None):
            #print('Calling listMarketCatalouge Operation to get MarketID and selectionId')
            now = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

            market_catalogue_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listMarketCatalogue", "params": {"filter":{"eventTypeIds":["' + eventTypeID + '"],"textQuery":"' + filter + '",'\
                '"marketStartTime":{"from":"' + now + '","to":"' + eventDateTime + \
                '"}},"sort":"FIRST_TO_START","maxResults":"1000", "marketProjection":["RUNNER_METADATA"]}, "id": 1}'
            """
            print(market_catalogue_req)
            """
            market_catalogue_response = self.callBettingAping(
                market_catalogue_req)
            """
            print(market_catalogue_response)
            """
            market_catalouge_loads = json.loads(market_catalogue_response)
            try:
                market_catalouge_results = market_catalouge_loads['result']
                return market_catalouge_results
            except:
                print('Exception from API-NG' +
                      str(market_catalouge_results['error']))
                # exit()
                return None

    def getMarketCatalogueForEvent(self, eventTypeID, eventId, turnInPlayEnabled):
        if (eventTypeID is not None):
            #print('Calling listMarketCatalouge Operation to get MarketID and selectionId')

            market_catalogue_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listMarketCatalogue", "params": {"filter":{"eventTypeIds":["' + eventTypeID + \
                '"],"eventIds":["' + eventId + \
                '"], "turnInPlayEnabled":"true"},"sort":"FIRST_TO_START","maxResults":"1000", "marketProjection":["RUNNER_METADATA"]}, "id": 1}'
            """
            print(market_catalogue_req)
            """
            market_catalogue_response = self.callBettingAping(
                market_catalogue_req)
            """
            print(market_catalogue_response)
            """
            market_catalouge_loads = json.loads(market_catalogue_response)
            try:
                market_catalouge_results = market_catalouge_loads['result']
                return market_catalouge_results
            except:
                print('Exception from API-NG' +
                      str(market_catalouge_results['error']))
                # exit()
                return None

    def listCurrentOrders(self, marketId=None):
        #print('Calling listCurrentOrders')

        try:
            if marketId is None:
                current_orders_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listCurrentOrders", "params": {"orderProjection":"ALL","dateRange":{}}, "id": 1}'
            else:
                current_orders_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listCurrentOrders", "params": {"marketIds":["' + \
                    marketId + '"],"orderProjection":"ALL","dateRange":{}}, "id": 1}'

            current_orders_response = self.callBettingAping(current_orders_req)
            """
            print(current_orders_response)
            """
            current_orders_loads = json.loads(current_orders_response)

            current_orders_results = current_orders_loads['result']
            return current_orders_results
        except:
            print('Exception from API-NG' +
                  str(current_orders_results['error']))
            # exit()
            return None

    def listEvents(self, eventTypeID, eventDateTime):
        #event_type_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listEvents", "params": {"filter":{ }}, "id": 1}'
        try:
            now = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            listEvents_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listEvents", "params": {"filter":{"eventTypeIds":["' + \
                eventTypeID + '"],''"marketStartTime":{"from":"' + now + \
                '","to":"' + eventDateTime + '"}},"maxResults":"1000"}, "id": 1}'

            listEventsResponse = self.callBettingAping(listEvents_req)
            eventLoads = json.loads(listEventsResponse)

            eventResults = eventLoads['result']
            return eventResults
        except:
            print('Exception from API-NG' + str(eventLoads['error']))
            # exit()
            return None
