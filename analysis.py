import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as pyspark_max, dense_rank
from pyspark.sql.window import Window

# Create a class for the Crash Analysis Application
class CrashAnalysisApp:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def task_1(self):
        # Task 1: Find the number of crashes (accidents) in which the number of persons killed are male
        primary_person_df = self.spark.read.csv(self.config["primary_person_csv_path"], header = True)

        # Filter and count crashes where the number of killed persons is male
        male_fatalities_count = primary_person_df.filter(
            (col("PRSN_GNDR_ID") == "MALE") &
            (col("PRSN_INJRY_SEV_ID") == "KILLED")
        ).select("CRASH_ID").distinct().count()

        return male_fatalities_count

    def task_2(self):
        # Task 2
        units_df = self.spark.read.csv(self.config["units_csv_path"], header = True)
        # Count two-wheelers involved in crashes
        two_wheelers_count = units_df.filter(col("VEH_BODY_STYL_ID").isin(["MOTORCYCLE","POLICE MOTORCYCLE"])).count()

        return two_wheelers_count
    
    def task_3(self):

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
    
    def run(self):
        # Task 1
        task_1_result = self.task_1()
        print(f"Task 1: Number of crashes with male fatalities: {task_1_result}")

        # Task 2
        task_2_result = self.task_2()
        print(f"Task 2: Number of two-wheelers involved in crashes: {task_2_result}")

        # Task 3
        task_3_result = self.task_3()
        print(f"Task 3: State with the highest number of accidents involving females: {task_3_result}")

        # Task 4
        task_4_result = self.task_4()
        print(f"Task 4: Top 5th to 15th VEH_MAKE_IDs with the largest number of injuries: {task_4_result}")

        # Task 5
        task_5_result = self.task_5()
        print("Task 5: Top ethnic user groups for each unique body style:")
        for body_style, result in task_5_result.items():
            print(f"  Body Style: {body_style}, Ethnicity: {result['ethnicity']}, Count: {result['count']}")

        # Task 6
        task_6_result = self.task_6()
        print(f"Task 6: Top 5 Zip Codes with the highest number of alcohol-related crashes: {task_6_result}")

        # Task 7
        task_7_result = self.task_7()
        print(f"Task 7: Count of distinct Crash IDs meeting specified criteria: {task_7_result}")

        # Task 8
        task_8_result = self.task_8()
        print(f"Task 8: Top 5 Vehicle Makes with specified criteria: {task_8_result}")


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
