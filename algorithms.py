
import os
import time
import pandas as pd
from apyori import apriori
from data_preparation import get_data_path


def run_apriori(min_support, min_confidence, min_lift, max_length):
    print(" -> Start preparing for Apriori algorithm")

    directory_path = get_data_path()

    file_name = "apriori_data_for_prediction"
    filepath = f"{directory_path}\\{file_name}.csv"

    if os.path.isfile(filepath):
        print(" -> Apriori_data_for_prediction already exists")

    else:
        data = pd.read_csv(f"{directory_path}\\apriori_data.csv")
        print(f' -> Number of unique id: {len(data["N_Id"].unique())}\n -> Start data cleaning')

        g = data.groupby("N_Id")

        sorted_data = []
        start = time.time()

        for name, group in g:
            sorted_data.append(group["N_Product"].values)

        for s in range(len(sorted_data)):
            sorted_data[s] = list(set(sorted_data[s]))

        end = time.time()
        print(f" -> Spent: {round(end - start, 2)} sec for cleaning apriori data")
        print(" -> Start running apriori algorithm\n -> This function usually takes 4-8 minutes")

        start = time.time()
        association_rules = apriori(sorted_data,
                                    min_support=min_support,
                                    min_confidence=min_confidence,
                                    min_lift=min_lift,
                                    max_length=max_length)

        association_results = list(association_rules)

        end = time.time()
        print()
        print(f" -> Spent: {round(end - start, 2)} sec for running apriori algorithm")
        print()

        X = []
        Y = []
        Support = []
        Confidence = []
        Lift = []

        for item in association_results:
            pair = item[0]
            items = [x for x in pair]
            print("Rule: " + items[0] + " -> " + items[1])
            print("Support: " + str(item[1]))
            print("Confidence: " + str(item[2][0][2]))
            print("Lift: " + str(item[2][0][3]))
            print("=====================================")
            X.append(items[0])
            Y.append(items[1])
            Support.append(item[1])
            Confidence.append(item[2][0][2])
            Lift.append(item[2][0][3])

        dataframe = {
            "X": X,
            "Y": Y,
            "Confidence": Confidence,
            "Support": Support,
            "Lift": Lift
        }

        df = pd.DataFrame(dataframe)

        print(f" -> Downloading the apriori_data_for_prediction into: {directory_path}")
        df.to_csv(f"{directory_path}\\{file_name}.csv", index=False)
        print(f" -> Successfully download the apriori_data_for_prediction into: {directory_path}")
