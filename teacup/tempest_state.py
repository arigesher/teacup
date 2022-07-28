from dataclasses import dataclass
import datetime
import json


@dataclass
class TempestState:
    station_pressure: float # MB
    air_temp: float  # Celcius
    relative_humidty: float # Percentage
    lightning_strikes_count: int 
    lightning_strike_average_distance: float # km
    wind_speed: float # mph
    wind_direction: float # degrees
    wind_direction_name: str 
    timestamp: datetime.datetime
    
    
    def update_from_rapid_wind(self, rapid_wind):
        pass


    def to_ws_json(self):
        out = dict(
            air_temp=self.air_temp*1.8+32,
            pressure=self.station_pressure,
            wind_speed=self.wind_speed,
            wind_direction=self.wind_direction,
            wind_direction_name=self.wind_direction_name,
            timestamp = self.timestamp.replace(second=0).isoformat()
        )
        return json.dumps(out)

