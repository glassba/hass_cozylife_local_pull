"""Define motor entities registered through Home Assistant standard platforms."""

from __future__ import annotations

from .const import MOTOR_COUNTDOWN
from .device import CozyLifeDevice
from .number import CozyLifeCountdown
from .switch import CozyLifeSwitch


class CozyLifeMotorSwitch(CozyLifeSwitch):
    """Represent a motor's DPID 1 start and stop control."""

    _turn_on_value = 1


class CozyLifeMotorCountdown(CozyLifeCountdown):
    """Represent a motor's DPID 6 local countdown."""

    def __init__(self, device: CozyLifeDevice) -> None:
        """Bind the shared countdown behavior to the motor data point."""
        super().__init__(device, MOTOR_COUNTDOWN, "Countdown")
