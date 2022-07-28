from dataclasses import asdict, fields
import datetime
import json

from pytest import fixture

from theresort.tempest.messages import AirObservation, RapidWind, SkyObservation, StationObservation, TempestState

"""
Test data from https://weatherflow.github.io/Tempest/api/udp/v143/
"""

@fixture
def sample_obs_air_json():
    return 	json.dumps({
	  "serial_number": "AR-00004049",
	  "type":"obs_air",
	  "hub_sn": "HB-00000001",
	  "obs":[[1493164835,835.0,10.0,45,1,7.2,3.46,1]],
	  "firmware_revision": 17
	})


@fixture
def sample_rapid_wind_json():
    return json.dumps(	{
	  "serial_number": "SK-00008453",
	  "type":"rapid_wind",
	  "hub_sn": "HB-00000001",
	  "ob":[1493322445,2.3,128]
	})


@fixture
def sample_obs_sky_json():
    return json.dumps(	{
	  "serial_number": "SK-00008453",
	  "type":"obs_sky",
	  "hub_sn": "HB-00000001",
	  "obs":[[1493321340,9000,10,0.0,2.6,4.6,7.4,187,3.12,1,130,None,0,3]],
	  "firmware_revision": 29
	})

@fixture
def sample_obs_st_json():
    return json.dumps(    {
        "serial_number": "ST-00000512",
        "type": "obs_st",
        "hub_sn": "HB-00013030",
        "obs": [
            [1588948614,0.18,0.22,0.27,144,6,1017.57,22.37,50.26,328,0.03,3,0.000000,0,0,0,2.410,1]
        ],
        "firmware_revision": 129
    })


def test_field_names():
    """
    Make sure all our field names agree
    """
    rapid_wind = {f.name for f in fields(RapidWind)}
    air_observation = {f.name for f in fields(AirObservation)}
    sky_observation = {f.name for f in fields(SkyObservation)}
    station_observation = {f.name for f in fields(StationObservation)}
    tempest_state = {f.name for f in fields(TempestState)}
    max_len = len(tempest_state)

    assert len(rapid_wind) <= max_len
    assert len(air_observation) <= max_len
    assert len(sky_observation) <= max_len
    assert len(station_observation) <= max_len

    assert rapid_wind - tempest_state == set()
    assert air_observation - tempest_state == set()
    assert sky_observation - tempest_state == set()

    assert tempest_state == rapid_wind | air_observation | sky_observation | station_observation

def test_air_observation(sample_obs_air_json):
    ao = AirObservation.from_msg(json.loads(sample_obs_air_json))
    assert ao.pressure == 835.0
    assert ao.air_temperature == 10
    assert ao.relative_humidity == 45
    assert ao.lightning_strikes_count == 1
    assert ao.lightning_strike_average_distance == 7.2
    assert ao.timestamp == datetime.datetime.fromtimestamp(1493164835)

def test_rapid_wind(sample_rapid_wind_json):
    rw = RapidWind.from_msg(json.loads(sample_rapid_wind_json))
    assert rw.timestamp == datetime.datetime.fromtimestamp(1493322445)
    assert rw.wind_speed == 2.3
    assert rw.wind_direction == 128

def test_sky_observation(sample_obs_sky_json):
    so = SkyObservation.from_msg(json.loads(sample_obs_sky_json))
    assert so.timestamp == datetime.datetime.fromtimestamp(1493321340)
    assert so.illuminance == 9000.0
    assert so.uv_index == 10
    assert so.rain_accumulated == 0.0
    assert so.wind_lull == 2.6
    assert so.wind_avg == 4.6
    assert so.wind_gust == 7.4
    assert so.wind_direction ==  187
    assert so.solar_radiation == 130
    assert so.rain_accumulated_day == None  
    assert so.precipitation_type == "none"

def test_station_observation(sample_obs_st_json):
    so = StationObservation.from_msg(json.loads(sample_obs_st_json))
    assert so.timestamp == datetime.datetime.fromtimestamp(1588948614)
    assert so.wind_lull == 0.18
    assert so.wind_avg == 0.22
    assert so.wind_gust == 0.27
    assert so.wind_direction == 144
    assert so.pressure == 1017.57
    assert so.air_temperature == 22.37
    assert so.relative_humidity == 50.26
    assert so.illuminance == 328
    assert so.uv_index == 0.03
    assert so.solar_radiation == 3
    assert so.rain_accumulated == 0
    assert so.precipitation_type == "none"
    assert so.lightning_strikes_count == 0
    assert so.lightning_strike_average_distance == 0


def test_check_state_updates(sample_obs_air_json, sample_rapid_wind_json, sample_obs_sky_json, sample_obs_st_json):
    
    so = StationObservation.from_msg(json.loads(sample_obs_st_json))
    t = TempestState.empty_state()
    for name in [f.name for f in fields(TempestState)]:
        assert getattr(t, name) == None 
    
    prev_state = TempestState(**asdict(t))
    t.update(so)
