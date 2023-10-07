import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring
from datetime import datetime

def print_function_execution_time(func):
    """
    Annotation function will run before and after and will give the execution details
    """
    def wrapper(*args, **kwargs):
        function_name = func.__name__
        start_time = datetime.now()
        print(f"{function_name} started at {start_time}")

        result = func(*args, **kwargs)

        end_time = datetime.now()
        execution_time = end_time - start_time
        print(f"{function_name} finished at {end_time} , Time Taken : {execution_time}")

        return result
    return wrapper

# Create a class for the Crash Analysis Application
class CrashAnalysisApp:
    """
    This is the main class all the tasks are its functions and can be executed separately
    """
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def write_to_output_file(self, task_name, result):
        output_file = self.config.get("output_file")
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(output_file, "a") as file:
            file.write(f"{current_datetime} - {task_name}: {result}\n")
    
    @print_function_execution_time
    def task_1(self):
        """
        Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
        """
        primary_person_df = self.spark.read.csv(self.config["primary_person_csv_path"], header = True)

        # Filter and count crashes where the number of killed persons is male
        male_fatalities_count = primary_person_df.filter(
            (col("PRSN_GNDR_ID") == "MALE") &
            (col("PRSN_INJRY_SEV_ID") == "KILLED")
        ).select("CRASH_ID").distinct().count()

        return male_fatalities_count

    @print_function_execution_time
    def task_2(self):
        """
        Analysis 2: How many two wheelers are booked for crashes? 
        """
        units_df = self.spark.read.csv(self.config["units_csv_path"], header = True)
        # Count two-wheelers involved in crashes
        two_wheelers_count = units_df.filter(col("VEH_BODY_STYL_ID").isin(["MOTORCYCLE","POLICE MOTORCYCLE"])).count()

        return two_wheelers_count
    
    @print_function_execution_time
    def task_3(self):
        """
        Analysis 3: Which state has highest number of accidents in which females are involved?
        """
        # Load the primary person data
        primary_person_df = self.spark.read.csv(self.config["primary_person_csv_path"], header = True)

        # Load the units data
        units_df = self.spark.read.csv(self.config["units_csv_path"], header = True)

        # Join primary person data with units data to get state information
        state_with_max_female_accidents = primary_person_df.join(units_df, on="CRASH_ID")\
            .groupBy("VEH_LIC_STATE_ID")\
            .agg({"CRASH_ID": "count"})\
            .withColumnRenamed("count(CRASH_ID)", "accident_count")\
            .orderBy(col("accident_count").desc())\
            .select("VEH_LIC_STATE_ID")\
            .first()["VEH_LIC_STATE_ID"]
        
        return state_with_max_female_accidents
    
    @print_function_execution_time
    def task_4(self):
        """
        Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        """

        # Load the primary person data
        primary_person_df = self.spark.read.csv(self.config["primary_person_csv_path"], header = True)
        units_df = self.spark.read.csv(self.config["units_csv_path"], header = True)

        # Filter and count injuries including death by vehicle make
        injury_counts_by_make = primary_person_df.join(units_df, on="CRASH_ID")\
            .filter(col("PRSN_INJRY_SEV_ID") != "NOT INJURED")\
            .groupBy("VEH_MAKE_ID")\
            .agg({"CRASH_ID": "count"})\
            .withColumnRenamed("count(CRASH_ID)", "injury_count")\
            .orderBy(col("injury_count").desc())

        # Calculate the top 5th to 15th VEH_MAKE_IDs
        top_makes = injury_counts_by_make.limit(15).subtract(injury_counts_by_make.limit(5))

        return [row["VEH_MAKE_ID"] for row in top_makes.collect()]
    
    @print_function_execution_time
    def task_5(self):
        """
        Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
        """

        primary_person_df = self.spark.read.csv(self.config["primary_person_csv_path"], header = True)
        units_df = self.spark.read.csv(self.config["units_csv_path"], header = True)

        # Group by body style and ethnicity to find the top user group for each body style
        top_ethnic_user_groups = primary_person_df.join(units_df, on="CRASH_ID")\
            .groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")\
            .agg({"CRASH_ID": "count"})\
            .withColumnRenamed("count(CRASH_ID)", "user_group_count")\
            .orderBy("VEH_BODY_STYL_ID", col("user_group_count").desc())

        # Collect the results
        results = {}
        for row in top_ethnic_user_groups.collect():
            body_style = row["VEH_BODY_STYL_ID"]
            ethnicity = row["PRSN_ETHNICITY_ID"]
            user_group_count = row["user_group_count"]

            if body_style not in results:
                results[body_style] = {"ethnicity": ethnicity, "count": user_group_count}

        return results
    
    @print_function_execution_time
    def task_6(self):
        """
        Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
        """

        primary_person_df = self.spark.read.csv(self.config["primary_person_csv_path"], header = True)
        units_df = self.spark.read.csv(self.config["units_csv_path"], header = True)

        # Filter and count crashes with alcohol as the contributing factor
        alcohol_related_crashes = primary_person_df.join(units_df, on="CRASH_ID")\
            .filter(
                (col("CONTRIB_FACTR_1_ID").contains("ALCOHOL")) |
                (col("CONTRIB_FACTR_2_ID").contains("ALCOHOL"))
            )

        # We only need cards
        crashed_cars = alcohol_related_crashes.filter(
            col("VEH_BODY_STYL_ID").contains("CAR") &
            col("DRVR_ZIP").isNotNull()
        )

        # Group by driver zip code and count crashes
        top_zip_codes = crashed_cars.groupBy("DRVR_ZIP")\
            .agg({"CRASH_ID": "count"})\
            .withColumnRenamed("count(CRASH_ID)", "crash_count")\
            .orderBy(col("crash_count").desc())\
            .limit(5)

        return [row["DRVR_ZIP"] for row in top_zip_codes.collect()]
    
    @print_function_execution_time
    def task_7(self):
        """
        Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        """

        units_df = self.spark.read.csv(self.config["units_csv_path"], header = True)
        # Filter crashes with no damaged property, damage level > 4, and car avails insurance
        filtered_crashes = units_df.filter(
            (substring(col("VEH_DMAG_SCL_1_ID"), -1, 1).cast("int") > 4) &
            (substring(col("VEH_DMAG_SCL_2_ID"), -1, 1).cast("int") > 4) &
            (col("FIN_RESP_TYPE_ID") == "PROOF OF LIABILITY INSURANCE")
        )

        # Count distinct crash IDs
        distinct_crash_count = filtered_crashes.select("CRASH_ID").distinct().count()

        return distinct_crash_count
    
    @print_function_execution_time
    def task_8(self):
        """
        Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, 
        has licensed Drivers, used top 10 used vehicle colours and has car licensed with the 
        Top 25 states with highest number of offences (to be deduced from the data)
        """

        primary_person_df = self.spark.read.csv(self.config["primary_person_csv_path"], header = True)
        units_df = self.spark.read.csv(self.config["units_csv_path"], header = True)

        # Filter drivers charged with speeding-related offenses
        speeding_offenses = primary_person_df.join(units_df, on = 'CRASH_ID')\
            .filter(
                (col("CONTRIB_FACTR_1_ID").contains("SPEED")) |
                (col("CONTRIB_FACTR_2_ID").contains("SPEED"))
            )

        # Ge the top 10 colors
        top_10_colors = units_df\
            .groupBy("VEH_COLOR_ID")\
            .count()\
            .orderBy(col("count").desc())\
            .limit(10)\
            .select("VEH_COLOR_ID")

        top_10_colors_list = [row["VEH_COLOR_ID"] for row in top_10_colors.collect()]

        # Filter drivers with licenses and top 10 used vehicle colors
        licensed_drivers_top_car_color = speeding_offenses.filter(
            (col("DRVR_LIC_TYPE_ID") == "DRIVER LICENSE") &
            (col("VEH_COLOR_ID").isin(top_10_colors_list))
        )

        # Filter cars licensed with the top 25 states with the highest number of offenses
        top_25_states = units_df.groupBy("VEH_LIC_STATE_ID")\
            .agg({"CRASH_ID": "count"})\
            .withColumnRenamed("count(CRASH_ID)", "offense_count")\
            .orderBy(col("offense_count").desc())\
            .limit(25)

        # Get the cars with top states
        cars_with_top_states = licensed_drivers_top_car_color.join(top_25_states, on="VEH_LIC_STATE_ID", how="inner")

        # Group by vehicle make and count offenses
        top_vehicle_makes = cars_with_top_states.groupBy("VEH_MAKE_ID")\
            .agg({"CRASH_ID": "count"})\
            .withColumnRenamed("count(CRASH_ID)", "offense_count")\
            .orderBy(col("offense_count").desc())\
            .limit(5)

        return [row["VEH_MAKE_ID"] for row in top_vehicle_makes.collect()]
    
    def run(self):
        """
        Function will call all the tasks one by one
        results will be stored in output/results.txt
        """
        # Task 1
        task_1_result = self.task_1()
        self.write_to_output_file("Task 1", f"Number of crashes with male fatalities: {task_1_result}")

        # Task 2
        task_2_result = self.task_2()
        self.write_to_output_file("Task 2", f"Number of two-wheelers involved in crashes: {task_2_result}")

        # Task 3
        task_3_result = self.task_3()
        self.write_to_output_file("Task 3", f"State with the highest number of accidents involving females: {task_3_result}")

        # Task 4
        task_4_result = self.task_4()
        self.write_to_output_file("Task 4", f"Top 5th to 15th VEH_MAKE_IDs with the largest number of injuries: {task_4_result}")

        # Task 5
        task_5_result = self.task_5()
        self.write_to_output_file("Task 5", "Top ethnic user groups for each unique body style:")
        for body_style, result in task_5_result.items():
            self.write_to_output_file("Task 5", f"  Body Style: {body_style}, Ethnicity: {result['ethnicity']}, Count: {result['count']}")

        # Task 6
        task_6_result = self.task_6()
        self.write_to_output_file("Task 6", f"Top 5 Zip Codes with the highest number of alcohol-related crashes: {task_6_result}")

        # Task 7
        task_7_result = self.task_7()
        self.write_to_output_file("Task 7", f"Count of distinct Crash IDs meeting specified criteria: {task_7_result}")

        # Task 8
        task_8_result = self.task_8()
        self.write_to_output_file("Task 8", f"Top 5 Vehicle Makes with specified criteria: {task_8_result}")

        print('Check results in output/results.txt')

def main():
    # Load configuration from config.json
    with open('config.json') as config_file:
        config = json.load(config_file)

    # Create a Spark session
    spark = SparkSession.builder.appName(config["app_name"]).getOrCreate()

    # Initialize the application
    app = CrashAnalysisApp(spark, config)

    # Run the application
    app.run()

    # stop spark
    spark.stop()

if __name__ == "__main__":
    main()
