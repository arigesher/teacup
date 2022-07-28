from dataclasses import fields
from theresort.tempest.messages import AirObservation, RapidWind, SkyObservation, StationObservation, TempestState
from theresort import __version__
from pytest import fixture
import json 

def test_version():
    assert __version__ == '0.1.0'

