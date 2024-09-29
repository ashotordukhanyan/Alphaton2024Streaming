import functools
import threading
import time
from datetime import timedelta

import csp
from csp import ts
from csp.impl.pushadapter import PushInputAdapter
from csp.impl.wiring import py_push_adapter_def
from typing import Dict, Tuple
from gbfs.client import GBFSClient
import logging

CITIBIKE_NY_URL = 'https://gbfs.citibikenyc.com/gbfs/gbfs.json'

@functools.lru_cache(maxsize=1)
def getGBFSClient(url,language='en'):
    return GBFSClient(url, 'en')

class GBFSStationInfo(csp.Struct):
    ''' Station static info - almost never changes '''
    name: str
    capacity: int
    station_id : str
    external_id :str
    lon: float
    lat: float

class GBFSStationStatus(csp.Struct):
    ''' Current station status - ticks a lot'''
    station_id : str
    num_bikes_available: int
    num_ebikes_available: int
    num_docks_available: int
    num_bikes_disabled: int
    num_docks_disabled: int


class GBFSStationInfoAdapterImpl(PushInputAdapter):
    def __init__(self, interval=timedelta(seconds=60)):
        logging.debug("GBFSStationInfoAdapterImpl::__init__")
        self._interval = interval
        self._thread = None
        self._running = False
        self._last_tick = None
        self._last_update_time = None
        self._client = getGBFSClient(CITIBIKE_NY_URL, 'en')

    def getNewData(self) -> Dict[str,GBFSStationInfo]:
        si = self._client.request_feed('station_information')
        updateTime = si['last_updated']
        if self._last_update_time is not None and updateTime <= self._last_update_time:
            return None
        else:
            self._last_update_time = updateTime
            newData = {}
            for station in si['data']['stations']:
                newData[station['station_id']] = GBFSStationInfo.from_dict(dict(name=station['name'], capacity=station['capacity'], \
                            station_id=station['station_id'], external_id=station['external_id'], lon=station['lon'], lat=station['lat']))
            if newData != self._last_tick:
                self._last_tick = newData
                return newData
            else:
                return None

    def start(self, starttime, endtime):
        logging.debug("GBFSStationInfoAdapterImpl::start")
        self._running = True
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def stop(self):
        logging.debug("GBFSStationInfoAdapterImpl::stop")
        if self._running:
            self._running = False
            self._thread.join()

    def _run(self):
        while self._running:
            logging.debug("GBFSStationInfoAdapterImpl::_run")
            data = self.getNewData()
            if data is not None and len(data):
                self.push_tick(data)
            time.sleep(self._interval.total_seconds())

class GBFSStationStatusAdapterImpl(PushInputAdapter):
    def __init__(self, interval=timedelta(seconds=60)):
        logging.debug("GBFSStationStatusAdapterImpl::__init__")
        self._interval = interval
        self._thread = None
        self._running = False
        self._last_update_time = None
        self._client = getGBFSClient(CITIBIKE_NY_URL, 'en')

    def getNewData(self) -> Dict[str,GBFSStationStatus]:
        si = self._client.request_feed('station_status')
        updateTime = si['last_updated']
        if self._last_update_time is not None and updateTime <= self._last_update_time:
            return None
        else:
            self._last_update_time = updateTime
            update = {}
            for s in si['data']['stations']:
                update[s['station_id']] = GBFSStationStatus.from_dict(dict(station_id=s['station_id'], num_bikes_available=s['num_bikes_available'], \
                            num_ebikes_available=s['num_ebikes_available'], num_docks_available=s['num_docks_available'], num_bikes_disabled=s['num_bikes_disabled'], \
                            num_docks_disabled=s['num_docks_disabled'] ))
            return update

    def start(self, starttime, endtime):
        logging.debug("GBFSStationStatusAdapterImpl::start")
        self._running = True
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def stop(self):
        logging.debug("GBFSStationStatusAdapterImpl::stop")
        if self._running:
            self._running = False
            self._thread.join()

    def _run(self):
        while self._running:
            logging.debug("GBFSStationStatusAdapterImpl::_run")
            data = self.getNewData()
            if data is not None and len(data):
                self.push_tick(data)
            time.sleep(self._interval.total_seconds())

GBFSStationInfoAdapter = py_push_adapter_def("GBFSStationInfoAdapter", GBFSStationInfoAdapterImpl, ts[Dict[str,GBFSStationInfo]], interval=timedelta)
GBFSStationStatusAdapter = py_push_adapter_def("GBFSStationStatusAdapter", GBFSStationStatusAdapterImpl, ts[Dict[str,GBFSStationStatus]], interval=timedelta)
