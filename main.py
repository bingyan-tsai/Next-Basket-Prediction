

from data_preparation import get_recency_table, get_frequency_table, get_monetary_table, create_tables, \
                             insert_datas, get_apriori_data, get_order_history, \
                             get_popular_prods, get_testing_data

from rfm_analysis import run_rfm_analysis
from data_prediction import prediction
from algorithms import run_apriori


if __name__ == '__main__':
    print(" -> Pre Flight")
    # for RFM analysis
    get_recency_table()
    get_frequency_table()
    get_monetary_table()
    run_rfm_analysis()

    # for database manipulation
    # create_tables()
    # insert_datas()

    # for apriori
    get_apriori_data()
    run_apriori(0.0005, 0.30, 4, 3)

    # for prediction
    for i in range(1, 9):
        file_name = f"rfm{i}"
        get_order_history(file_name)
        get_popular_prods(file_name)
        get_testing_data(file_name)

        # length of prediction list
        prediction(16, 16, file_name)
    print(" -> End Flight")
