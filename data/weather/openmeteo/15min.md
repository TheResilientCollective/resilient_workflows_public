for forecasting, we can get 15 minute data.

https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&minutely_15=temperature_2m,relative_humidity_2m,precipitation,wind_gusts_10m,wind_speed_10m,is_day,wind_direction_10m,dew_point_2m&timezone=America%2FLos_Angeles

multisite:https://api.open-meteo.com/v1/forecast?latitude=32.567097,32.576139,32.552794&longitude=-117.090656,-117.115361,-117.047286&minutely_15=temperature_2m,relative_humidity_2m,precipitation,wind_gusts_10m,wind_speed_10m,is_day,wind_direction_10m,dew_point_2m&timezone=America%2FLos_Angeles&forecast_days=3&elevation=0,0,0


```json
{
  "latitude": 52.52,
  "longitude": 13.41,
  "generationtime_ms": 0.20194053649902344,
  "utc_offset_seconds": -25200,
  "timezone": "America/Los_Angeles",
  "timezone_abbreviation": "PDT",
  "elevation": 38.0,
  "minutely_15": {
    "time": [
      "2024-06-01T00:00",
      "2024-06-01T00:15",
      ...
    ],
    "temperature_2m": [
      18.3,
      18.1,
      ...
    ],
    "relative_humidity_2m": [
      60,
      62,
      ...
    ],
    ...
  }
}
```
