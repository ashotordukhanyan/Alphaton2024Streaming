from queue import SimpleQueue
import csp# as tsp
from csp import ts
import logging
from typing import Dict, List
import pandas as pd
import haversine
from datetime import timedelta, datetime

from csp.impl.wiring.threaded_runtime import ThreadRunner

from dataloader.gbfsadapter import GBFSStationInfoAdapter, GBFSStationStatusAdapter, GBFSStationInfo, GBFSStationStatus

class AngelRoute(csp.Struct):
    ''' Represents a route from a (nearly) full to a (nearly) empty bike sharing station'''
    origin: GBFSStationInfo
    destination: GBFSStationInfo
    originStatus: GBFSStationStatus
    destinationStatus: GBFSStationStatus
    distance: float

@csp.node
def station_universe(stationInfo: ts[Dict[str, GBFSStationInfo]]) -> ts[List[str]]:
    if csp.ticked(stationInfo):
        return sorted(list(stationInfo.keys()))


@csp.node
def stationStatusDynBasket(stationStatus:ts[Dict[str,GBFSStationStatus]]) -> csp.DynamicBasket[str, GBFSStationStatus]:
    ''' Produces a dynamic basket of station status ( available bikes etc. ) keyed by station id'''
    with csp.state():
        _sow_status = {}
    if csp.ticked(stationStatus) :
        changes = {}
        for k,v in stationStatus.items():
            if k not in _sow_status or _sow_status[k] != v:
                changes[k] = v
                _sow_status[k] = v
        if len(changes) > 0:
            csp.output(changes)

@csp.node
def stationOccupancyRates(stationStatus:csp.DynamicBasket[str,GBFSStationStatus]) -> csp.DynamicBasket[str,float]:
    ''' Produces a dynamic basket of station occupancy rates (0=no bikes,1=full) keyed by station id. -1 indicates disabled station'''
    if csp.ticked(stationStatus.shape) or csp.ticked(stationStatus):
        changes = {}
        changed_keys = list(stationStatus.shape.added) if csp.ticked(stationStatus.shape) else \
            [ k for k, v in stationStatus.tickeditems() ]
        for key in changed_keys:
            v = stationStatus[key]
            if v.num_bikes_available + v.num_docks_available == 0:
                occr = -1 #special sentinel for disabled stations ( I think ).
            else:
                occr = 0 if v.num_bikes_available == 0 else v.num_bikes_available/(v.num_bikes_available + v.num_docks_available)
            changes[key] = occr
        if csp.ticked(stationStatus.shape):
            for key in stationStatus.shape.removed:
                logging.warning('Station %s removed from status data',key)
                csp.remove_dynamic_key(key)
        if len(changes) > 0:
            return changes

@csp.node
def distances(stationIds: ts[List[str]], stationInfo: ts[Dict[str,GBFSStationInfo]]) -> ts[pd.DataFrame]:
    ''' Produces a distance matrix between all stations in stationIds using haversine distance in feet '''
    if csp.ticked(stationInfo,stationIds) and csp.valid(stationIds,stationInfo):
        # Calculate haversine distances between all stations and return them as a symmetric square matrix
        coordinates = [( stationInfo[x].lat, stationInfo[x].lon ) for x in stationIds]
        distances = haversine.haversine_vector(coordinates, coordinates, unit=haversine.Unit.FEET, comb=True)
        return pd.DataFrame(data=distances, index=stationIds, columns=stationIds)


@csp.node
def broadcast_rides(rides:ts[List[AngelRoute]], outQ:SimpleQueue):
    ''' Broadcasts the rides to the global queue '''
    if csp.ticked(rides):
        if outQ is not None:
            outQ.put([r.to_dict() for r in rides])

@csp.node
def angel_opportunities(stationOccupancyRates:csp.DynamicBasket[str,float], stationDistances:ts[pd.DataFrame],
                        stationInfo:ts[Dict[str,GBFSStationInfo]],stationStatus:csp.DynamicBasket[str,GBFSStationStatus],
                        fullOR:float = .95, emptyOR : float = .05, maxResults : int = 20) -> ts[List[AngelRoute]]:

    if csp.ticked(stationOccupancyRates,stationDistances,stationInfo) and csp.valid(stationOccupancyRates,stationDistances,stationInfo):
        stationIds = stationDistances.columns.to_list()
        full_stations = [ sid for sid in stationIds if sid in stationOccupancyRates and stationOccupancyRates[sid] >= fullOR]
        empty_stations = [ sid for sid in stationIds if sid in stationOccupancyRates and stationOccupancyRates[sid] <= emptyOR \
                           and stationOccupancyRates[sid] != -1]
        full2EmptyRoutes = stationDistances.loc[full_stations, empty_stations].idxmin(axis=1)
        full2EmptyDistances = stationDistances.loc[full_stations, empty_stations].min(axis=1)

        potentialRides = pd.DataFrame()
        potentialRides['Destination'] = full2EmptyRoutes
        potentialRides['Distance'] = full2EmptyDistances
        potentialRides.index.name = 'Origin'
        potentialRides.sort_values('Distance', inplace=True)

        routes = potentialRides.reset_index().to_records(index=False)[0:maxResults]
        #package the results now
        results = []
        for origin,destination,distance in routes:
            results.append(AngelRoute(origin=stationInfo[origin],destination=stationInfo[destination],
                                      originStatus=stationStatus[origin],destinationStatus=stationStatus[destination],
                                      distance=distance))

        return results

@csp.graph
def angel_main( outQ: SimpleQueue = None):
    stationInfos = GBFSStationInfoAdapter(timedelta(seconds = 30))
    stationIds = station_universe(stationInfos)
    distanceMatrix = distances(stationIds, stationInfos)
    #csp.print("info", distanceMatrix)
    statuses = GBFSStationStatusAdapter(timedelta(seconds=30))
    statusesDynBasket = stationStatusDynBasket(statuses)
    occupancyRates = stationOccupancyRates(statusesDynBasket)
    rides = angel_opportunities(occupancyRates, distanceMatrix, stationInfos,statusesDynBasket)
    broadcast_rides(rides, outQ)
    #csp.print('Rides',rides)


def runit( outQ : SimpleQueue = None ) -> ThreadRunner:
    runner = csp.run_on_thread(angel_main, outQ, realtime=True,starttime=datetime.utcnow())
    return runner

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,format='%(thread)d %(asctime)s - %(levelname)s - %(message)s')
    runner = runit()
    runner.join()