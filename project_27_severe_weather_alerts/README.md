# Project 27: Severe Weather Alert Pipeline

## Difficulty: Hard
## Time Limit: 150 min

## Scenario
The National Weather Service needs a real-time pipeline that ingests readings from thousands of weather stations, correlates multiple measurement types (temperature, wind, precipitation), and generates composite severe weather alerts. When dangerous combinations appear — extreme heat with high wind, or heavy precipitation — alerts must escalate automatically.

## Problem Statement
Build a PySpark Structured Streaming pipeline that reads weather station measurements from Google Pub/Sub, joins multiple measurement types within time windows, evaluates composite alert rules, and escalates when multiple alert conditions overlap in the same region.

## Input Schema
| Field | Type | Description |
|-------|------|-------------|
| station_id | string | Weather station identifier |
| measurement_type | string | Type: temperature/wind_speed/humidity/precipitation/pressure |
| value | double | Measured value |
| unit | string | Unit of measurement (C, km/h, %, mm/hr, hPa) |
| region | string | Geographic region |
| county | string | County name |
| state | string | US state abbreviation |
| alert_level | string | Station-reported level (none/advisory/watch/warning) |
| event_time | timestamp | Measurement timestamp |

## Pub/Sub Configuration
- **Project ID:** `your-gcp-project-id` (replace with yours)
- **Topic:** `spark-27-weather-alerts`
- **Subscription:** `spark-27-weather-alerts-sub`

## Requirements
1. Join measurements by region within 10-minute tumbling windows to correlate temperature, wind, and precipitation readings.
2. Apply composite alert rules:
   - `extreme_heat_warning`: temperature > 40C AND wind_speed > 60 km/h in same region/window
   - `flood_warning`: precipitation > 50 mm/hr in same region/window
   - `severe_storm`: wind_speed > 100 km/h OR (wind_speed > 80 km/h AND precipitation > 30 mm/hr)
   - `blizzard_warning`: temperature < -10C AND wind_speed > 50 km/h AND precipitation > 10 mm/hr
3. Escalate alert when 2+ different alert types fire for the same region in the same window (severity = "critical").
4. Include all contributing measurements as evidence.
5. Handle late data up to 5 minutes late with watermarking.

## Expected Output Schema
| Field | Type | Description |
|-------|------|-------------|
| region | string | Affected region |
| state | string | State |
| window_start | timestamp | Alert window start |
| window_end | timestamp | Alert window end |
| alert_type | string | Alert classification |
| severity | string | Severity (warning/critical) |
| contributing_stations | array[string] | Stations triggering the alert |
| measurements | object | Key measurement values |
| is_escalated | boolean | True if multiple alert types overlap |

## Evaluation Criteria
| Criteria | Weight | Description |
|----------|--------|-------------|
| Correctness | 40% | Accurate multi-source joins and composite rule evaluation |
| Stream handling | 25% | Tumbling windows, watermarking, multi-type correlation |
| Code quality | 20% | Modular rule engine, clean join logic |
| Fault tolerance | 15% | Checkpointing, handling missing measurement types |

## Constraints
- Use PySpark Structured Streaming (not DStreams)
- Read from Google Pub/Sub using spark-sql-pubsub connector
- Handle late data arriving up to 5 minutes late
- Output mode: append

## Hints (optional, reveal if stuck)
<details>
<summary>Hint 1</summary>
Pivot the measurement_type column so each row in a window has temperature, wind_speed, etc. as separate columns before applying rules.
</details>
<details>
<summary>Hint 2</summary>
Use `groupBy(window("event_time", "10 minutes"), "region")` with pivot or conditional aggregations (max temperature, max wind, max precipitation).
</details>
<details>
<summary>Hint 3</summary>
For escalation, count distinct alert_types per region per window and flag as critical when count >= 2.
</details>
