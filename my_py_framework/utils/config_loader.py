import logging
import configparser


class ConfigReader:
    """
    A class to read configuration settings from a file and incorporate logging.
    """

    def __init__(self, config_filename):
        self.config_filename = config_filename
        self.config = configparser.ConfigParser()
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s %(name)s %(levelname)s: %(message)s",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler("config_reader.log"),
            ],
        )
        self.logger.info(f"Initialized ConfigReader for '{config_filename}'")

    def read_config(self):
        """
        Reads the configuration file and returns a dictionary of settings.
        """
        try:
            self.config.read(self.config_filename)
            self.logger.info(
                f"Successfully read configuration from '{self.config_filename}'"
            )
            config_dict = {
                section: dict(self.config.items(section))
                for section in self.config.sections()
            }
            return config_dict
        except FileNotFoundError:
            self.logger.error(f"Configuration file not found: '{self.config_filename}'")
            return None
        except configparser.Error as e:
            self.logger.error(
                f"Error reading configuration file '{self.config_filename}': {e}"
            )
            return None

    def get_setting(self, section, key):
        """
        Retrieves a specific configuration setting from the loaded configuration.

        Args:
            section (str): The name of the section in the configuration file.
            key (str): The name of the key within the section.

        Returns:
            str or None: The value of the setting, or None if the section or key is not found.
        """
        try:
            value = self.config.get(section, key)
            self.logger.debug(
                f"Retrieved setting '{key}' from section '{section}': {value}"
            )
            return value
        except (configparser.NoSectionError, configparser.NoOptionError):
            self.logger.warning(f"Setting '{key}' not found in section '{section}'")
            return None
