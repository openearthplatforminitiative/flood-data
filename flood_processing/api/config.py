class GloFASAPIConfig:
    BASE_CONFIG = {
        'system_version': 'operational',
        'hydrological_model': 'lisflood',
        'product_type': 'ensemble_perturbed_forecasts',
        'variable': 'river_discharge_in_the_last_24_hours',
        'format': 'grib'
    }

    def __init__(self, year, month, day, leadtime_hour, area):
        self.year = year
        self.month = month
        self.day = day
        self.leadtime_hour = leadtime_hour
        self.area = area

    def to_dict(self):
        """Returns the config as a dictionary"""
        config = self.BASE_CONFIG.copy()
        config.update({
            'year': self.year,
            'month': self.month,
            'day': self.day,
            'leadtime_hour': self.leadtime_hour,
            'area': self.area
        })
        return config
