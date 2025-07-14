

https://sandiego.onerain.com/map/?view=c2a3b028-f616-44eb-acb2-8ae127a6470e#

Getting  data from a site
https://sandiego.onerain.com/export/file/?site_id=5&site=79f55a5c-21a9-471f-8716-e66d81d5ea93&device_id=5&device=bafc154d-7081-4170-b693-ed5cd0fe7db8&mode=&hours=&data_start=2025-04-21%2009:49:41&data_end=2025-04-28%2009:49:41&tz=US%2FPacific&format_datetime=%25Y-%25m-%25d+%25H%3A%25i%3A%25S&mime=txt&delimiter=comma

        query parameters

site: 79f55a5c-21a9-471f-8716-e66d81d5ea93
device_id: 5
device: bafc154d-7081-4170-b693-ed5cd0fe7db8
mode: 
hours: 
data_start: 2025-04-21 09:49:41
data_end: 2025-04-28 09:49:41
tz: US/Pacific
format_datetime: %Y-%m-%d %H:%i:%S
mime: txt
delimiter: comma

JSON Site Data:
https://sandiego.onerain.com/export/flot/?method=sensorDetails&site_id=5&device_id=2&site=5&device=2&range=standard&bin=86400&time_zone=US%2FPacific&_=1745859233924
method: sensorDetails
site_id: 5
device_id: 2
site: 5
device: 2
range: standard
bin: 86400
time_zone: US/Pacific
_: 1745859233924

sitedata stream level
https://sandiego.onerain.com/export/file/?site_id=5&site=79f55a5c-21a9-471f-8716-e66d81d5ea93&device_id=2&device=a4df830d-d803-4137-b008-bc1a1397f17d&mode=&hours=&data_start=2025-04-21%2009:53:53&data_end=2025-04-28%2009:53:53&tz=US%2FPacific&format_datetime=%25Y-%25m-%25d+%25H%3A%25i%3A%25S&mime=txt&delimiter=comma
site_id: 5
site: 79f55a5c-21a9-471f-8716-e66d81d5ea93
device_id: 2
device: a4df830d-d803-4137-b008-bc1a1397f17d
mode: 
hours: 
data_start: 2025-04-21 09:53:53
data_end: 2025-04-28 09:53:53
tz: US/Pacific
format_datetime: %Y-%m-%d %H:%i:%S
mime: txt
delimiter: comma


Sites:
flow volume TJ Tiver
site_id=5&device_id=5
stream level: https://sandiego.onerain.com/sensor/?site_id=5&site=79f55a5c-21a9-471f-8716-e66d81d5ea93&device_id=2&device=a4df830d-d803-4137-b008-bc1a1397f17d

San Ysidro (weatehr only)
https://sandiego.onerain.com/site/?site_id=6&site=a0a9ea9e-7922-4240-8428-95c0c2c7f3b3

Goat Caynon
https://sandiego.onerain.com/site/?site_id=8&site=87ce910e-0827-45b0-a142-44530a808bed


flot (json):
https://sandiego.onerain.com/export/flot/?method=sensorDetails&site_id=5&device_id=2&site=5&device=2&range=standard&bin=86400&time_zone=US%2FPacific&_=1745871508591
method: sensorDetails
site_id: 5
device_id: 2
site: 5
device: 2
range: standard
bin: 86400
time_zone: US/Pacific
_: 1745871508591

bin is seconds:
86400 - day
3600 -hour
2419200 - 28 days
2592000 - 30 days

# Sites: https://sandiego.onerain.com/site/?
## Smugglers Gulch 
site_id=7&site=2f198436-e967-4c4a-b713-44276102b345
stream level = device_id=5&device=671f71fb-f2b0-426a-a834-0b52ea04e7d8
method: sensorDetails
site_id: 7
device_id: 5
site: 7
device: 5

second stream level:
method: sensorDetails
site_id: 7
device_id: 3
site: 7
device: 3

## Goat Caynon
site_id=8&site=87ce910e-0827-45b0-a142-44530a808bed
### Stream Level 1:  (6684)
method: sensorDetails
site_id: 8
device_id: 5
site: 8
device: 5
range: standard

### Stream Level 2: (6682)
method: sensorDetails
site_id: 8
device_id: 3
site: 8
device: 3
range: Last 30 Days

## Tijuana Estuary (6700)
?site_id=9&site=a8283365-1925-4cb0-917e-dd1106b0509b
### Wind Direction (6696) 
method: sensorDetails
site_id: 9
device_id: 4
site: 9
device: 4
range: standard
### Relative Humidity (6694)
method: sensorDetails
site_id: 9
device_id: 2
site: 9
device: 2
range: Last 30 Days
### Peak Wind (6698)
not really worth is, last one was 90 days


## Tijuana R @ US Border (27065)
site_id=5&site=79f55a5c-21a9-471f-8716-e66d81d5ea93
### flow cfs
method: sensorDetails
site_id: 5
device_id: 5
site: 5
device: 5
### Stream Level (7)
method: sensorDetails
site_id: 5
device_id: 2
site: 5
device: 2
### Stage (ft) (7)
method: sensorDetails
site_id: 5
device_id: 4
site: 5
device: 4

## San Ysidro (66)
site_id=6&site=a0a9ea9e-7922-4240-8428-95c0c2c7f3b3
### Wind Direction (6985)
method: sensorDetails
site_id: 6
device_id: 10
site: 6
device: 10
### Average Wind Speed (6984)
method: sensorDetails
site_id: 6
device_id: 9
site: 6
device: 9
### Relative Humidity (6978)
method: sensorDetails
site_id: 6
device_id: 4
site: 6
device: 4
###  Peak Wind (6983)
method: sensorDetails
site_id: 6
device_id: 8
site: 6
device: 8




FLOT Sensor:

method: sensor
site_id: 7
device_id: 5
site: 2f198436-e967-4c4a-b713-44276102b345
device: 671f71fb-f2b0-426a-a834-0b52ea04e7d8


