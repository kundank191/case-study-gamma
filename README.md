# Case Studdy Gamma

The Application is a Python script built on Apache Spark that performs various analytics tasks on crash data. This application reads data from CSV files, processes it using PySpark, and generates insights based on the specified tasks.

## Table of Contents

- [Case Studdy Gamma](#case-studdy-gamma)
  - [Table of Contents](#table-of-contents)
  - [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
  - [Usage](#usage)
  - [Tasks](#tasks)
  - [Output](#output)

## Getting Started

This section provides an overview of how to get started with the Crash Analysis Application.

### Prerequisites

Before running the application, make sure you have the following prerequisites installed:

- Python
- Java

### Installation

1. Clone this repository to your local machine:

   ```bash
   git clone https://github.com/kundank191/case-study-gamma.git
   ```

2. Navigate to the project directory:

   ```bash
   cd case-study-gamma
   ```

3. Install the required dependencies:

   ```bash
   pytnon -m venv venv
   source venv/Scripts/activate
   pip install -r requirements.txt
   ```

## Usage

To use the Crash Analysis Application, follow these steps:

1. Update configuration file `config.json`

2. Run the application:

   ```bash
   spark-submit analysis.py
   ```

3. The application will execute various tasks and generate results in the specified output file.

## Tasks

The Crash Analysis Application performs the following tasks:

1. Find the number of crashes (accidents) in which the number of persons killed are male.
2. Count the number of two-wheelers involved in crashes.
3. Determine the state with the highest number of accidents involving females.
4. Identify the top 5th to 15th VEH_MAKE_IDs that contribute to the largest number of injuries.
5. Find the top ethnic user groups for each unique body style involved in crashes.
6. Identify the top 5 zip codes with the highest number of alcohol-related crashes.
7. Count the distinct Crash IDs where no damaged property was observed, the damage level is above 4, and the car has liability insurance.
8. Determine the top 5 vehicle makes where drivers are charged with speeding-related offenses, have licensed drivers, used the top 10 vehicle colors, and have cars licensed with the top 25 states with the highest number of offenses.

## Output

The results of each task are stored in the output file specified in the `config.json` file. The output file contains details about each task, including start and end times, execution times, and the task results. Here is an example of the output:

```
2023-10-07 20:33:08 - Task 1: Number of crashes with male fatalities: 180
2023-10-07 20:33:09 - Task 2: Number of two-wheelers involved in crashes: 784
2023-10-07 20:33:12 - Task 3: State with the highest number of accidents involving females: TX
2023-10-07 20:33:15 - Task 4: Top 5th to 15th VEH_MAKE_IDs with the largest number of injuries: ['HONDA', 'NA', 'GMC', 'JEEP', 'HYUNDAI', 'KIA', 'CHRYSLER', 'FREIGHTLINER', 'MAZDA', 'UNKNOWN']
2023-10-07 20:33:19 - Task 5: Top ethnic user groups for each unique body style:
2023-10-07 20:33:19 - Task 5:   Body Style: AMBULANCE, Ethnicity: WHITE, Count: 97
2023-10-07 20:33:19 - Task 5:   Body Style: BUS, Ethnicity: HISPANIC, Count: 391
2023-10-07 20:33:19 - Task 5:   Body Style: FARM EQUIPMENT, Ethnicity: WHITE, Count: 63
2023-10-07 20:33:19 - Task 5:   Body Style: FIRE TRUCK, Ethnicity: WHITE, Count: 112
2023-10-07 20:33:19 - Task 5:   Body Style: MOTORCYCLE, Ethnicity: WHITE, Count: 848
2023-10-07 20:33:19 - Task 5:   Body Style: NA, Ethnicity: WHITE, Count: 5693
2023-10-07 20:33:19 - Task 5:   Body Style: NEV-NEIGHBORHOOD ELECTRIC VEHICLE, Ethnicity: WHITE, Count: 10
2023-10-07 20:33:19 - Task 5:   Body Style: NOT REPORTED, Ethnicity: HISPANIC, Count: 2
2023-10-07 20:33:19 - Task 5:   Body Style: OTHER  (EXPLAIN IN NARRATIVE), Ethnicity: WHITE, Count: 459
2023-10-07 20:33:19 - Task 5:   Body Style: PASSENGER CAR, 2-DOOR, Ethnicity: WHITE, Count: 9877
2023-10-07 20:33:19 - Task 5:   Body Style: PASSENGER CAR, 4-DOOR, Ethnicity: WHITE, Count: 58312
2023-10-07 20:33:19 - Task 5:   Body Style: PICKUP, Ethnicity: WHITE, Count: 38609
2023-10-07 20:33:19 - Task 5:   Body Style: POLICE CAR/TRUCK, Ethnicity: WHITE, Count: 366
2023-10-07 20:33:19 - Task 5:   Body Style: POLICE MOTORCYCLE, Ethnicity: HISPANIC, Count: 3
2023-10-07 20:33:19 - Task 5:   Body Style: SPORT UTILITY VEHICLE, Ethnicity: WHITE, Count: 33902
2023-10-07 20:33:19 - Task 5:   Body Style: TRUCK, Ethnicity: WHITE, Count: 4204
2023-10-07 20:33:19 - Task 5:   Body Style: TRUCK TRACTOR, Ethnicity: WHITE, Count: 5815
2023-10-07 20:33:19 - Task 5:   Body Style: UNKNOWN, Ethnicity: WHITE, Count: 1178
2023-10-07 20:33:19 - Task 5:   Body Style: VAN, Ethnicity: WHITE, Count: 5291
2023-10-07 20:33:19 - Task 5:   Body Style: YELLOW SCHOOL BUS, Ethnicity: WHITE, Count: 264
2023-10-07 20:33:22 - Task 6: Top 5 Zip Codes with the highest number of alcohol-related crashes: ['76010', '75052', '78521', '75067', '76017']
2023-10-07 20:33:24 - Task 7: Count of distinct Crash IDs meeting specified criteria: 788
2023-10-07 20:33:29 - Task 8: Top 5 Vehicle Makes with specified criteria: ['FORD', 'CHEVROLET', 'TOYOTA', 'DODGE', 'HONDA']
2023-10-07 20:37:33 - Task 1: Number of crashes with male fatalities: 180
2023-10-07 20:37:33 - Task 2: Number of two-wheelers involved in crashes: 784
2023-10-07 20:37:36 - Task 3: State with the highest number of accidents involving females: TX
2023-10-07 20:37:38 - Task 4: Top 5th to 15th VEH_MAKE_IDs with the largest number of injuries: ['HONDA', 'NA', 'GMC', 'JEEP', 'HYUNDAI', 'KIA', 'CHRYSLER', 'FREIGHTLINER', 'MAZDA', 'UNKNOWN']
2023-10-07 20:37:41 - Task 5: Top ethnic user groups for each unique body style:
2023-10-07 20:37:41 - Task 5:   Body Style: AMBULANCE, Ethnicity: WHITE, Count: 97
2023-10-07 20:37:41 - Task 5:   Body Style: BUS, Ethnicity: HISPANIC, Count: 391
2023-10-07 20:37:41 - Task 5:   Body Style: FARM EQUIPMENT, Ethnicity: WHITE, Count: 63
2023-10-07 20:37:41 - Task 5:   Body Style: FIRE TRUCK, Ethnicity: WHITE, Count: 112
2023-10-07 20:37:41 - Task 5:   Body Style: MOTORCYCLE, Ethnicity: WHITE, Count: 848
2023-10-07 20:37:41 - Task 5:   Body Style: NA, Ethnicity: WHITE, Count: 5693
2023-10-07 20:37:41 - Task 5:   Body Style: NEV-NEIGHBORHOOD ELECTRIC VEHICLE, Ethnicity: WHITE, Count: 10
2023-10-07 20:37:41 - Task 5:   Body Style: NOT REPORTED, Ethnicity: HISPANIC, Count: 2
2023-10-07 20:37:41 - Task 5:   Body Style: OTHER  (EXPLAIN IN NARRATIVE), Ethnicity: WHITE, Count: 459
2023-10-07 20:37:41 - Task 5:   Body Style: PASSENGER CAR, 2-DOOR, Ethnicity: WHITE, Count: 9877
2023-10-07 20:37:41 - Task 5:   Body Style: PASSENGER CAR, 4-DOOR, Ethnicity: WHITE, Count: 58312
2023-10-07 20:37:41 - Task 5:   Body Style: PICKUP, Ethnicity: WHITE, Count: 38609
2023-10-07 20:37:41 - Task 5:   Body Style: POLICE CAR/TRUCK, Ethnicity: WHITE, Count: 366
2023-10-07 20:37:41 - Task 5:   Body Style: POLICE MOTORCYCLE, Ethnicity: HISPANIC, Count: 3
2023-10-07 20:37:41 - Task 5:   Body Style: SPORT UTILITY VEHICLE, Ethnicity: WHITE, Count: 33902
2023-10-07 20:37:41 - Task 5:   Body Style: TRUCK, Ethnicity: WHITE, Count: 4204
2023-10-07 20:37:41 - Task 5:   Body Style: TRUCK TRACTOR, Ethnicity: WHITE, Count: 5815
2023-10-07 20:37:41 - Task 5:   Body Style: UNKNOWN, Ethnicity: WHITE, Count: 1178
2023-10-07 20:37:41 - Task 5:   Body Style: VAN, Ethnicity: WHITE, Count: 5291
2023-10-07 20:37:41 - Task 5:   Body Style: YELLOW SCHOOL BUS, Ethnicity: WHITE, Count: 264
2023-10-07 20:37:44 - Task 6: Top 5 Zip Codes with the highest number of alcohol-related crashes: ['76010', '75052', '78521', '75067', '76017']
2023-10-07 20:37:45 - Task 7: Count of distinct Crash IDs meeting specified criteria: 788
2023-10-07 20:37:49 - Task 8: Top 5 Vehicle Makes with specified criteria: ['FORD', 'CHEVROLET', 'TOYOTA', 'DODGE', 'HONDA']
```