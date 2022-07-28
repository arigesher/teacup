
from dataclasses import asdict, dataclass
import datetime
import logging
from typing import Any, Dict, Optional

"""
API taken from https://weatherflow.github.io/Tempest/api/udp/v143/
"""

log = logging.getLogger("tempest.messages")

def _precipitation_type_name(precipitation_type):
    if precipitation_type == 0:
        return "none"
    elif precipitation_type == 1:
        return "rain"
    elif precipitation_type == 2:
        return "hail"
    raise ValueError(f"Unknown precipitation type: {precipitation_type}")


def _wind_direction(direction):
    if direction < 22.5 or direction > 337.5:
        return ("North", "N")
    elif direction >= 22.5 and direction < 67.5:
        return ("Northeast", "NE")
    elif direction >= 67.5 and direction < 112.5:
        return ("East", "E")
    elif direction >= 112.5 and direction < 157.5:
        return ("Southeast", "SE")
    elif direction >= 157.5 and direction < 202.5:
        return ("South", "S")
    elif direction >= 202.5 and direction < 247.5:
        return ("Southwest", "SW")
    elif direction >= 247.5 and direction < 292.5:
        return ("West", "W")
    elif direction >= 292.5 and direction < 337.5:
        return ("Northwest", "NW")
    return ("ERROR: {}".format(direction), "ER")


@dataclass
class RapidWind:
    """
    0	Time Epoch	Seconds
    1	Wind Speed	mps
    2	Wind Direction	Degrees
    """
    wind_speed: float
    wind_direction: float 
    timestamp: datetime.datetime

    @classmethod
    def from_msg(cls, rapid_wind_msg) -> "RapidWind":
        _wind_speed = float(rapid_wind_msg["ob"][1])
        _wind_direction = float(rapid_wind_msg["ob"][2])
        _timestamp = datetime.datetime.fromtimestamp(float(rapid_wind_msg["ob"][0]))
        return RapidWind(wind_speed=_wind_speed,
                         wind_direction=_wind_direction,
                         timestamp=_timestamp)
    

@dataclass
class AirObservation:
    '''
    0	Time Epoch	Seconds
    1	Station Pressure	MB
    2	Air Temperature	C
    3	Relative Humidity	%
    4	Lightning Strike Count	
    5	Lightning Strike Avg Distance	km
    6	Battery	
    7	Report Interval	Minutes
    '''
    pressure: float # MB
    air_temperature: float  # Celcius
    relative_humidity: float # Percentage
    lightning_strikes_count: int 
    lightning_strike_average_distance: float # km
    timestamp: datetime.datetime    

    @classmethod
    def from_msg(cls, obs_air_msg: dict) -> "AirObservation":
        _observations = obs_air_msg["obs"][0]
        assert len(obs_air_msg["obs"]) == 1
        _timestamp = datetime.datetime.fromtimestamp(_observations[0])
        _air_pressure = float(_observations[1])
        _air_temperature = float(_observations[2])
        _humidity = float(_observations[3])
        _lightning_count = int(_observations[4])
        _lightning_avg_distance = float(_observations[5])

        return AirObservation(pressure=_air_pressure,
                              air_temperature=_air_temperature,
                              relative_humidity=_humidity,
                              lightning_strikes_count=_lightning_count,
                              lightning_strike_average_distance=_lightning_avg_distance,
                              timestamp=_timestamp)

@dataclass
class SkyObservation:
    '''
    0	Time Epoch	Seconds
    1	Illuminance	Lux
    2	UV	Index
    3	Rain amount over previous minute	mm
    4	Wind Lull (minimum 3 second sample)	m/s
    5	Wind Avg (average over report interval)	m/s
    6	Wind Gust (maximum 3 second sample)	m/s
    7	Wind Direction	Degrees
    8	Battery	Volts
    9	Report Interval	Minutes
    10	Solar Radiation	W/m^2
    11	Local Day Rain Accumulation	mm
    12	Precipitation Type	0 = none, 1 = rain, 2 = hail
    13	Wind Sample Interval	seconds
    '''
    timestamp: datetime.datetime
    illuminance: float
    uv_index: float 
    rain_accumulated: float 
    wind_lull: float 
    wind_avg: float
    wind_gust: float
    wind_direction: float 
    solar_radiation: float 
    rain_accumulated_day: Optional[float]
    precipitation_type: str 

    @classmethod
    def from_msg(cls, obs_sky_msg: Dict[str, Any]) -> "SkyObservation":
        _observations = obs_sky_msg["obs"][0]
        assert len(obs_sky_msg["obs"]) == 1
        _timestamp = datetime.datetime.fromtimestamp(_observations[0])
        _illuminance = float(_observations[1])
        _uv_index = float(_observations[2])
        _incremental_rain = float(_observations[3])
        _wind_lull = float(_observations[4])
        _wind_avg = float(_observations[5])
        _wind_gust = float(_observations[6])
        _wind_direction = float(_observations[7])
        _solar_radiation = float(_observations[10])
        _rain_accumulated = _observations[11]
        if _rain_accumulated is not None:
            _rain_accumulated = float(_rain_accumulated)

        _precipitation_type = _precipitation_type_name(int(_observations[12]))
        
        return SkyObservation(timestamp=_timestamp,
                              illuminance=_illuminance,
                              uv_index=_uv_index,
                              rain_accumulated=_incremental_rain,
                              wind_lull=_wind_lull,
                              wind_avg=_wind_avg,
                              wind_gust=_wind_gust,
                              wind_direction=_wind_direction,
                              solar_radiation=_solar_radiation,
                              rain_accumulated_day=_rain_accumulated,
                              precipitation_type=_precipitation_type)



@dataclass
class StationObservation:
    """
    0	Time Epoch	Seconds
    1	Wind Lull (minimum 3 second sample)	m/s
    2	Wind Avg (average over report interval)	m/s
    3	Wind Gust (maximum 3 second sample)	m/s
    4	Wind Direction	Degrees
    5	Wind Sample Interval	seconds
    6	Station Pressure	MB
    7	Air Temperature	C
    8	Relative Humidity	%
    9	Illuminance	Lux
    10	UV	Index
    11	Solar Radiation	W/m^2
    12	Rain amount over previous minute	mm
    13	Precipitation Type	0 = none, 1 = rain, 2 = hail, 3 = rain + hail (experimental)
    14	Lightning Strike Avg Distance	km
    15	Lightning Strike Count	
    16	Battery	Volts
    17	Report Interval	Minutes
    """
    timestamp: datetime.datetime
    wind_lull: float
    wind_avg: float
    wind_gust: float
    wind_direction: float
    pressure: float 
    air_temperature: float
    relative_humidity: float
    illuminance: float 
    uv_index: float
    solar_radiation: float 
    rain_accumulated: float 
    precipitation_type: str
    lightning_strikes_count: int 
    lightning_strike_average_distance: float # km

    @classmethod
    def from_msg(cls, obs_st_msg) -> "StationObservation":
        _observations = obs_st_msg["obs"][0]
        assert len(obs_st_msg["obs"]) == 1
        _timestamp = datetime.datetime.fromtimestamp(_observations[0])
        _wind_lull = float(_observations[1])
        _wind_avg = float(_observations[2])
        _wind_gust = float(_observations[3])
        _wind_direction = float(_observations[4])
        _air_pressure = float(_observations[6])
        _air_temperature = float(_observations[7])
        _humidity = float(_observations[8])
        _illuminance = float(_observations[9])
        _uv_index = float(_observations[10])
        _solar_radiation = float(_observations[11])
        _rain_accumulated = float(_observations[12])
        _precipitation_type = _precipitation_type_name(_observations[13])
        _lightning_count = int(_observations[15])
        _lightning_avg_distance = float(_observations[14])

        return StationObservation(timestamp=_timestamp,
                                  wind_lull=_wind_lull,
                                  wind_avg=_wind_avg,
                                  wind_gust=_wind_gust,
                                  wind_direction=_wind_direction,
                                  pressure=_air_pressure,
                                  air_temperature=_air_temperature,
                                  relative_humidity=_humidity,
                                  illuminance=_illuminance,
                                  uv_index=_uv_index,
                                  solar_radiation=_solar_radiation,
                                  rain_accumulated=_rain_accumulated,
                                  precipitation_type=_precipitation_type,
                                  lightning_strikes_count=_lightning_count,
                                  lightning_strike_average_distance=_lightning_avg_distance)


@dataclass
class TempestState(AirObservation, RapidWind, SkyObservation, StationObservation):
    pass 

    def update(self, new_data: AirObservation | RapidWind | SkyObservation | StationObservation)-> "TempestState":
        tempest_dict = asdict(self)
        tempest_dict.update(asdict(new_data))
        return TempestState(**tempest_dict)

    @classmethod
    def empty_state(cls):
        t = TempestState(*([None] * 17))
        return t

EVENT_TO_CONTAINER = {
    "obs_air": AirObservation,
    "rapid_wind": RapidWind,
    "obs_sky": SkyObservation,
    "obs_st": StationObservation
}

def get_message_object(tempest_msg):
    message_type = tempest_msg["type"]
    dataclass = EVENT_TO_CONTAINER.get(message_type)
    if dataclass is not None:
        return dataclass.from_msg(tempest_msg)
    else:
        log.debug("Message type not handled: {}".format(message_type))
        return None     
        