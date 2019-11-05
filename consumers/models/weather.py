"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        print(f"Topic [{message.topic()}] ---- Value [{message.value()}]")
        try:
            data = json.loads(message.value())
            print(f"Temperature: {data['temperature']}, Status: {data['status']}")
            self.temperature = value["temperature"]
            self.status = value["status"]
        except Exception as e:
            logger.fatal(f"Weather problems: {data}, {e}")
