
import os
import time
import pandas as pd
from data_preparation import get_data_path


def run_rfm_analysis():
    directory_path = get_data_path()

    start = time.time()
    print(" -> Start running RFM analysis")

    r = pd.read_csv(f"{directory_path}\\recency_table.csv")
    f = pd.read_csv(f"{directory_path}\\frequency_table.csv")
    m = pd.read_csv(f"{directory_path}\\monetary_table.csv")

    print(" -> Start data cleaning")

    d1 = pd.merge(left=r, right=f, left_on='N_Customer', right_on='N_Customer')
    data = pd.merge(left=d1, right=m, left_on='N_Customer', right_on='N_Customer')

    recency = []
    frequency = []
    monetary = []

    med_r = data["Recency"].median()
    med_f = data["Frequency"].median()
    med_m = data["Monetary"].median()

    for d in data["Recency"]:
        if d >= med_r:
            recency.append(1)
        else:
            recency.append(0)

    for d in data["Frequency"]:
        if d >= med_f:
            frequency.append(1)
        else:
            frequency.append(0)

    for d in data["Monetary"]:
        if d >= med_m:
            monetary.append(1)
        else:
            monetary.append(0)

    data["Recency"] = recency
    data["Frequency"] = frequency
    data["Monetary"] = monetary

    rfm = []

    for d in range(len(data)):
        rfm.append(str(data["Recency"][d]) + str(data["Frequency"][d]) + str(data["Monetary"][d]))

    data["RFM"] = rfm
    data = data[["N_Customer", "RFM"]]

    label = []

    for d in data["RFM"]:
        if d == "111":
            label.append("重要價值客戶")
        elif d == "001":
            label.append("重要挽留客戶")
        elif d == "101":
            label.append("重要深耕客戶")
        elif d == "011":
            label.append("重要喚回客戶")
        elif d == "100":
            label.append("新客戶")
        elif d == "110":
            label.append("潛力客戶")
        elif d == "010":
            label.append("一般維持客戶")
        else:
            label.append("流失客戶")

    data["RFM"] = label

    # loading full dataset
    print(" -> Successfully conducted the process of data cleaning")
    print(" -> Start loading the full dataset")

    root_directory = os.getcwd()
    directory_path = os.path.join(root_directory, "original_data")
    full_data = pd.read_csv(f"{directory_path}\\carrefour.csv")

    # directory for RFM analysis data
    directory_path_rfm = os.path.join(root_directory, "rfm_labeled_data")
    os.makedirs(directory_path_rfm, exist_ok=True)

    print(" -> Start merging the RFM clusters' data with the full dataset")

    # Check if data already exists
    filepath = f"{directory_path_rfm}\\rfm1.csv"

    if os.path.isfile(filepath):
        print(" -> Data of first RFM cluster already exists")

    else:
        # 重要價值客戶
        rfm1 = pd.merge(left=data[data["RFM"] == "重要價值客戶"],
                        right=full_data, left_on='N_Customer', right_on='n_customer')

        print(f" -> Downloading the data of first RFM cluster into: {directory_path_rfm}")

        rfm1.to_csv(f"{directory_path_rfm}\\rfm1.csv", index=False)
        print(f" -> Successfully download the data of first RFM cluster into: {directory_path_rfm}")

    filepath = f"{directory_path_rfm}\\rfm2.csv"

    if os.path.isfile(filepath):
        print(" -> Data of second RFM cluster already exists")

    else:
        # 重要挽留客戶
        rfm2 = pd.merge(left=data[data["RFM"] == "重要挽留客戶"],
                        right=full_data, left_on='N_Customer', right_on='n_customer')

        print(f" -> Downloading the data of second RFM cluster into: {directory_path_rfm}")

        rfm2.to_csv(f"{directory_path_rfm}\\rfm2.csv", index=False)
        print(f" -> Successfully download the data of second RFM cluster into: {directory_path_rfm}")

    filepath = f"{directory_path_rfm}\\rfm3.csv"

    if os.path.isfile(filepath):
        print(" -> Data of third RFM cluster already exists")

    else:
        # 重要深耕客戶
        rfm3 = pd.merge(left=data[data["RFM"] == "重要深耕客戶"],
                        right=full_data, left_on='N_Customer', right_on='n_customer')

        print(f" -> Downloading the data of third RFM cluster into: {directory_path_rfm}")

        rfm3.to_csv(f"{directory_path_rfm}\\rfm3.csv", index=False)
        print(f" -> Successfully download the data of third RFM cluster into: {directory_path_rfm}")

    filepath = f"{directory_path_rfm}\\rfm4.csv"

    if os.path.isfile(filepath):
        print(" -> Data of fourth RFM cluster already exists")

    else:
        # 重要喚回客戶
        rfm4 = pd.merge(left=data[data["RFM"] == "重要喚回客戶"],
                        right=full_data, left_on='N_Customer', right_on='n_customer')

        print(f" -> Downloading the data of fourth RFM cluster into: {directory_path_rfm}")

        rfm4.to_csv(f"{directory_path_rfm}\\rfm4.csv", index=False)
        print(f" -> Successfully download the data of fourth RFM cluster into: {directory_path_rfm}")

    filepath = f"{directory_path_rfm}\\rfm5.csv"

    if os.path.isfile(filepath):
        print(" -> Data of fifth RFM cluster already exists")
    else:
        # 新客戶
        rfm5 = pd.merge(left=data[data["RFM"] == "新客戶"],
                        right=full_data, left_on='N_Customer', right_on='n_customer')

        print(f" -> Downloading the data of fifth RFM cluster into: {directory_path_rfm}")

        rfm5.to_csv(f"{directory_path_rfm}\\rfm5.csv", index=False)
        print(f" -> Successfully download the data of fifth RFM cluster into: {directory_path_rfm}")

    filepath = f"{directory_path_rfm}\\rfm6.csv"

    if os.path.isfile(filepath):
        print(" -> Data of sixth RFM cluster already exists")

    else:
        # 潛力客戶
        rfm6 = pd.merge(left=data[data["RFM"] == "潛力客戶"],
                        right=full_data, left_on='N_Customer', right_on='n_customer')

        print(f" -> Downloading the data of sixth RFM cluster into: {directory_path_rfm}")

        rfm6.to_csv(f"{directory_path_rfm}\\rfm6.csv", index=False)
        print(f" -> Successfully download the data of sixth RFM cluster into: {directory_path_rfm}")

    filepath = f"{directory_path_rfm}\\rfm7.csv"

    if os.path.isfile(filepath):
        print(" -> Data of seventh RFM cluster already exists")

    else:
        # 一般維持客戶
        rfm7 = pd.merge(left=data[data["RFM"] == "一般維持客戶"],
                        right=full_data, left_on='N_Customer', right_on='n_customer')

        print(f" -> Downloading the data of seventh RFM cluster into: {directory_path_rfm}")

        rfm7.to_csv(f"{directory_path_rfm}\\rfm7.csv", index=False)
        print(f" -> Successfully download the data of seventh RFM cluster into: {directory_path_rfm}")

    filepath = f"{directory_path_rfm}\\rfm8.csv"

    if os.path.isfile(filepath):
        print(" -> Data of eighth RFM cluster already exists")

    else:
        # 流失客戶
        rfm8 = pd.merge(left=data[data["RFM"] == "流失客戶"],
                        right=full_data, left_on='N_Customer', right_on='n_customer')

        print(f" -> Downloading the data of eighth RFM cluster into: {directory_path_rfm}")

        rfm8.to_csv(f"{directory_path_rfm}\\rfm8.csv", index=False)
        print(f" -> Successfully download the data of eighth RFM cluster into: {directory_path_rfm}")

    end = time.time()
    print(f" -> Spent: {round(end - start, 2)} sec for RFM analysis")
