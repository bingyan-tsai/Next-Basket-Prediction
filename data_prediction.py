
import time
import pandas as pd
from tqdm import tqdm
from data_preparation import get_data_path, get_result_path

# Apriori
x_list = []
y_list = []

# Prediction
dict_of_f1_score = {}


def load_data(file_name):

    name_list = []
    sorted_data = []
    sorted_purchase = []

    directory_path = get_data_path()

    # for history order data of each RFM cluster
    start = time.time()
    print(" -> Start loading customers' history order data")
    data = pd.read_csv(f"{directory_path}\\{file_name}_order_history.csv")

    # Grouped data by customers
    grouped_data = data.groupby("N_Customer")

    for name, group in grouped_data:
        # 各顧客的歷史購物排序
        name_list.append(name)
        sorted_data.append(group["N_Product"].values)

    end = time.time()
    print(f" -> Spent: {round(end - start, 2)} sec for loading customers' history order data")
    print(" -> Start loading popular products' list")

    # for popular products of each RFM cluster
    start = time.time()
    data = pd.read_csv(f"{directory_path}\\{file_name}_popular_prods_list.csv")

    pop_list = list(data["N_Product"])

    end = time.time()
    print(f" -> Spent: {round(end - start, 2)} sec for loading popular products' list")
    print(" -> Start loading testing data")

    # for testing data of each RFM cluster
    start = time.time()
    test = pd.read_csv(f"{directory_path}\\{file_name}_testing_data.csv")

    test_grouped = test.groupby("N_Customer")

    for name, group in test_grouped:
        # 各顧客在最後一單所購買的商品
        sorted_purchase.append(group["N_Product"].values)

    end = time.time()
    print(f" -> Spent: {round(end - start, 2)} sec for loading testing data")
    print(" -> Start loading Apriori data")

    start = time.time()
    apri = pd.read_csv(f"{directory_path}\\apriori_data_for_prediction.csv",)

    for x, y in apri.groupby("X"):
        x_list.append(x)
        y_list.append(y["Y"].values)

    end = time.time()
    print(f" -> Spent: {round(end - start, 2)} sec for loading Apriori data")
    print(" -> All datasets have been loaded, start prediction")

    apri_dict = dict(zip(x_list, y_list))

    return file_name, apri_dict, name_list, pop_list, sorted_data, sorted_purchase


def prediction(start, end, file_name, popular_count=10):
    file_name, apri_dict, name_list, pop_list, sorted_data, sorted_purchase = load_data(file_name)
    directory_path = get_result_path()

    # combine the prediction list with Apriori list
    print(" -> Start to combine the prediction list with Apriori list")
    for n in tqdm(range(len(sorted_data))):
        for s in sorted_data[n]:
            # 確認商品是否在Apriori的X清單中
            if s in x_list:
                # 若有則加入Apriori的關聯商品
                sorted_data[n] = list(sorted_data[n]) + list(apri_dict[s])

    print(" -> Successfully combined the prediction list with Apriori list")

    # combine the prediction list with Apriori list
    print(" -> Start to combine the prediction list with popular products' list")
    for n in tqdm(range(len(sorted_data))):
        sorted_data[n] = list(sorted_data[n]) + pop_list[:popular_count]

    print(" -> Successfully combined the prediction list with popular products' list")

    # start prediction
    list_of_f1_score = []
    for i in range(start, end+1):
        print(f"\n\n\n【當前單一顧客的商品猜測數】：{i} 個")

        # 猜測數量
        predict_num = i

        amount = 0
        for n in tqdm(range(len(sorted_purchase))):
            amount += len(sorted_purchase[n])

        # 最後一單的商品數量(TP+FN)
        print(f" -> {file_name} 的最後一單共有: {amount} 個商品")

        predict = 0
        for n in tqdm(range(len(sorted_data))):
            # 加總猜測商品數
            predict += len(sorted_data[n][:predict_num])

        # 猜測的商品數量(TP+FP)
        print(f" -> 模型針對 {file_name} 總共猜測了: {predict} 個商品")

        count = 0
        for n in tqdm(range(len(sorted_purchase))):
            for d in sorted_data[n][:predict_num]:
                if d in sorted_purchase[n]:
                    count += 1

        # 猜中的商品數量(TP)
        print(f" -> 在模型預測的商品中，總共猜中: {count} 個商品")
        # 猜中的商品數量/猜測的商品數量 = TP/(TP+FP)
        precision = count / predict
        print(f" -> Precision: {round((count / predict), 4) * 100}%")
        print(" -> 註：Precision = 猜中的商品數量/猜測的商品數量 = TP/(TP+FP)")

        # 猜中的商品數量/最後一單的商品數量 = TP/(TP+FN)
        recall = count / amount
        print(f" -> Recall: {round((count / amount), 4) * 100}%")
        print(" -> 註：Recall = 猜中的商品數量/最後一單的商品數量 = TP/(TP+FN)")

        # F1-score
        f1_score = (2*precision*recall)/(precision+recall)
        list_of_f1_score.append(f1_score)
        print(f" -> F1-score: {round(f1_score, 4) * 100}%")
        print(" -> 註：F1-score = (2*precision*recall)/(precision+recall)")

    # 觀察每個RFM分群預測的最佳長度
    dict_of_f1_score[file_name] = list_of_f1_score

    # create the file of final prediction
    print(f" -> Downloading the prediction of {file_name} into: {directory_path}")
    # product length of each customers' prediction list
    predict_num = end
    for n in tqdm(range(len(sorted_data))):
        sorted_data[n] = sorted_data[n][:predict_num]

    dataframe = pd.DataFrame({"N_Customer": name_list, "Next Purchase": sorted_data})
    print()
    print(dataframe)
    print()

    dataframe.to_csv(f"{directory_path}\\{file_name}_final_prediction.csv", index=False)
    print(f" -> Successfully download the prediction of {file_name} into: {directory_path}")
    print(pd.DataFrame(dict_of_f1_score))
