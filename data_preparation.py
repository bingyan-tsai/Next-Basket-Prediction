
import os
import time
import pymysql
import pandas as pd


def get_data_path():
    root_directory = os.getcwd()
    directory_path = os.path.join(root_directory, "data")
    os.makedirs(directory_path, exist_ok=True)

    return directory_path


def get_result_path():
    root_directory = os.getcwd()
    directory_path = os.path.join(root_directory, "result")
    os.makedirs(directory_path, exist_ok=True)

    return directory_path


# Change your settings of database here
db_settings = {
    "host": "Hostname",
    "port": "Port",
    "user": "Username",
    "password": "Password",
    "db":       "Dasebase_name",
    "charset":  "utf8"
}


def get_recency_table():
    print(" -> Start fetching customers' recency table")

    directory_path = get_data_path()
    filepath = f"{directory_path}\\recency_table.csv"

    # Check if data already exists
    if os.path.isfile(filepath):
        print(" -> Customers' recency table already exists")

    else:
        start = time.time()
        print(f" -> Querying {db_settings['db']}(database) for customers' recency table")

        try:
            conn = pymysql.connect(**db_settings)

            with conn.cursor() as cursor:
                # Change your table here
                query = '''
                    SELECT DISTINCT n_customer, CAST(MAX(order_date) AS DATE) AS last_order_day,
                                    -- 取得最後一單-今天的日期數
                                    DATEDIFF("2021-12-31", CAST(MAX(order_date) AS DATE)) AS recency
                    FROM carrefour_sales
                    WHERE order_date IN (
                        SELECT max(order_date)
                        FROM carrefour_sales
                        GROUP BY n_customer
                    )
                    GROUP BY n_customer
                    ORDER BY n_customer ASC;
                    '''

                cursor.execute(query)
                conn.commit()

                result = cursor.fetchall()

                cursor.close()
                conn.close()

        except Exception as ex:
            print(ex)

        end = time.time()

        dataframe = pd.DataFrame(result, columns=["N_Customer", "Last_Order_Day", "Recency"])
        print()
        print(dataframe)
        print()

        print(f" -> Spent: {round(end - start, 2)} sec on query")
        print(f" -> Downloading the customers' recency table into: {directory_path}")

        dataframe.to_csv(f"{directory_path}\\recency_table.csv", index=False)
        print(f" -> Successfully download the customers' recency table into: {directory_path}")


def get_frequency_table():
    print(" -> Start fetching customers' frequency table")

    directory_path = get_data_path()
    filepath = f"{directory_path}\\frequency_table.csv"

    # Check if data already exists
    if os.path.isfile(filepath):
        print(" -> Customers' frequency table already exists")

    else:
        start = time.time()
        print(f" -> Querying {db_settings['db']}(database) for customers' frequency table")

        try:
            conn = pymysql.connect(**db_settings)

            with conn.cursor() as cursor:
                # Change your table here
                query = '''
                    SELECT n_customer, COUNT(n_id) AS frequency
                    FROM (
                        SELECT DISTINCT n_customer, n_id
                        FROM carrefour_sales
                    ) AS sub_query
                    GROUP BY n_customer
                    ORDER BY n_customer;
                    '''

                cursor.execute(query)
                conn.commit()

                result = cursor.fetchall()

                cursor.close()
                conn.close()

        except Exception as ex:
            print(ex)

        end = time.time()

        dataframe = pd.DataFrame(result, columns=["N_Customer", "Frequency"])
        print()
        print(dataframe)
        print()

        print(f" -> Spent: {round(end - start, 2)} sec on query")
        print(f" -> Downloading the customers' frequency table into: {directory_path}")

        dataframe.to_csv(f"{directory_path}\\frequency_table.csv", index=False)
        print(f" -> Successfully download the customers' frequency table into: {directory_path}")


def get_monetary_table():
    print(" -> Start fetching customers' monetary table")

    directory_path = get_data_path()
    filepath = f"{directory_path}\\monetary_table.csv"

    # Check if data already exists
    if os.path.isfile(filepath):
        print(" -> Customers' monetary table already exists")

    else:
        start = time.time()
        print(f" -> Querying {db_settings['db']}(database) for customers' monetary table")

        try:
            conn = pymysql.connect(**db_settings)

            with conn.cursor() as cursor:
                # Change your table here
                query = '''
                    SELECT n_customer, SUM(sum_price) AS monetary
                    FROM carrefour_sales
                    GROUP BY n_customer
                    ORDER BY n_customer;
                    '''

                cursor.execute(query)
                conn.commit()

                result = cursor.fetchall()

                cursor.close()
                conn.close()

        except Exception as ex:
            print(ex)

        end = time.time()

        dataframe = pd.DataFrame(result, columns=["N_Customer", "Monetary"])
        print()
        print(dataframe)
        print()

        print(f" -> Spent: {round(end - start, 2)} sec on query")
        print(f" -> Downloading the customers' monetary table into: {directory_path}")

        dataframe.to_csv(f"{directory_path}\\monetary_table.csv", index=False)
        print(f" -> Successfully download the customers' monetary table into: {directory_path}")


# For database manipulation
def create_table(table_name):
    try:
        conn = pymysql.connect(**db_settings)

        with conn.cursor() as cursor:

            query = f'''
            CREATE TABLE {table_name}(
                order_date   TIMESTAMP,
                n_id         VARCHAR(50),
                n_city       VARCHAR(20),
                n_district   VARCHAR(20),
                n_store      VARCHAR(20),
                n_sex        VARCHAR(20),
                n_age_group  VARCHAR(20),
                n_customer   VARCHAR(20),
                rfm_label    VARCHAR(20),
                n_department VARCHAR(20),
                n_product    VARCHAR(20),
                sales_price  INT,
                quantity     INT,
                sum_price    INT
                );

                '''

            cursor.execute(query)
            conn.commit()

            cursor.close()
            conn.close()
            print(f"Successfully created table: {table_name}")

    except Exception as ex:
        print(ex)


def create_tables():
    for i in range(1, 9):
        table_name = f"rfm{i}"
        create_table(table_name)


def insert_data(table_name):
    root_directory = os.getcwd()
    directory_path_rfm = os.path.join(root_directory, "rfm_labeled_data")

    start = time.time()
    print(f" -> Start inserting data into {table_name}")
    data = pd.read_csv(f"{directory_path_rfm}\\{table_name}.csv", low_memory=False)

    try:
        conn = pymysql.connect(**db_settings)

        with conn.cursor() as cursor:

            command = f"""
            INSERT INTO {table_name}(order_date, n_id, n_city, n_district, n_store, 
                                     n_sex, n_age_group, n_customer, rfm_label,
                                     n_department, n_product, sales_price, quantity, sum_price)

            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            n = len(data["n_id"])
            print(f"{table_name}共有 {n} 筆資料")

            for i in range(len(data["n_id"])):
                cursor.execute(command, (data["order_date"][i],
                                         data["n_id"][i],
                                         data["n_city"][i],
                                         data["n_district"][i],
                                         data["n_store"][i],
                                         data["n_sex"][i],
                                         data["n_age_group"][i],
                                         data["n_customer"][i],
                                         data["RFM"][i],
                                         data["n_department"][i],
                                         data["n_product"][i],
                                         data["sales_price"][i],
                                         data["quantity"][i],
                                         data["sum_price"][i],))

                if i % 10000 == 0:
                    print(f"正在匯入第 {i} 筆資料")

            conn.commit()

            cursor.close()
            conn.close()
            print(f" -> Successfully inserted data into {table_name}")

    except Exception as exception:
        print(exception)

    end = time.time()
    print(f" -> Spent: {round(end - start, 2)} sec for inserting data into {table_name}")


def insert_datas():
    for i in range(1, 9):
        table_name = f"rfm{i}"
        insert_data(table_name)


def get_order_history(file_name):
    print(" -> Start fetching customers' order history")

    directory_path = get_data_path()
    filepath = f"{directory_path}\\{file_name}_order_history.csv"

    # Check if data already exists
    if os.path.isfile(filepath):
        print(" -> Customers' order history already exists")

    else:
        start = time.time()
        print(f" -> Querying {db_settings['db']}(database) for customers' order history")

        try:
            conn = pymysql.connect(**db_settings)

            with conn.cursor() as cursor:
                # Change your table here
                query = f'''
                    SELECT *
                    FROM(
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY n_customer) AS row_num
                    FROM
                    (SELECT N_Customer, N_Product, COUNT(N_Product) AS Product_Count
                    FROM  {file_name}
                    GROUP BY N_Customer, N_Product
                    ORDER BY N_Customer ASC, Product_Count DESC) AS SUB_QUERY1
                    ORDER BY N_Customer ASC, Product_Count DESC
                    ) AS SUB_QUERY2
                    WHERE row_num <= 20;
                    '''

                cursor.execute(query)
                conn.commit()

                result = cursor.fetchall()

                cursor.close()
                conn.close()

        except Exception as ex:
            print(ex)

        end = time.time()

        dataframe = pd.DataFrame(result, columns=["N_Customer", "N_Product", "Product_Count", "Rank"])
        print()
        print(dataframe)
        print()

        print(f" -> Spent: {round(end - start, 2)} sec on query")
        print(f" -> Downloading the order history of {file_name} into: {directory_path}")

        dataframe.to_csv(f"{directory_path}\\{file_name}_order_history.csv", index=False)
        print(f" -> Successfully download the order history of {file_name} into: {directory_path}")


def get_popular_prods(file_name):
    print(" -> Start fetching popular products' list")

    directory_path = get_data_path()
    filepath = f"{directory_path}\\{file_name}_popular_prods_list.csv"

    # Check if data already exists
    if os.path.isfile(filepath):
        print(" -> Popular products' list already exists")

    else:
        start = time.time()
        print(f" -> Querying {db_settings['db']}(database) for popular products' list")

        try:
            conn = pymysql.connect(**db_settings)

            with conn.cursor() as cursor:
                # Change your table here
                query = f'''
                    SELECT n_product, COUNT(n_product) AS n_count
                    FROM {file_name}
                    GROUP BY n_product
                    ORDER BY n_count DESC
                    LIMIT 30;
                    '''

                cursor.execute(query)
                conn.commit()

                result = cursor.fetchall()

                cursor.close()
                conn.close()

        except Exception as ex:
            print(ex)

        end = time.time()

        dataframe = pd.DataFrame(result, columns=["N_Product", "Product_Count"])
        print()
        print(dataframe)
        print()

        print(f" -> Spent: {round(end - start, 2)} sec on query")
        print(f" -> Downloading the popular products' list of {file_name} into: {directory_path}")

        dataframe.to_csv(f"{directory_path}\\{file_name}_popular_prods_list.csv", index=False)
        print(f" -> Successfully download the popular products' list of {file_name} into: {directory_path}")


def get_testing_data(file_name):
    print(" -> Start fetching testing data")

    directory_path = get_data_path()
    filepath = f"{directory_path}\\{file_name}_testing_data.csv"

    if os.path.isfile(filepath):
        print(" -> Testing data already exists")

    else:
        start = time.time()
        print(f" -> Querying {db_settings['db']}(database) for testing data")

        try:
            conn = pymysql.connect(**db_settings)

            with conn.cursor() as cursor:
                # Change your table here
                # Find out the last order of each customer
                query = f'''
                    SELECT DISTINCT n_customer, order_date, n_product
                    FROM {file_name}
                    WHERE order_date IN (
                        SELECT max(order_date)
                        FROM {file_name}
                        GROUP BY n_customer
                    )
                    ORDER BY n_customer ASC, n_product ASC;
                    '''

                cursor.execute(query)
                conn.commit()

                result = cursor.fetchall()

                cursor.close()
                conn.close()

        except Exception as ex:
            print(ex)

        end = time.time()

        dataframe = pd.DataFrame(result, columns=["N_Customer", "Last_Order_Date", "N_Product"])
        print()
        print(dataframe)
        print()

        print(f" -> Spent: {round(end - start, 2)} sec for query")
        print(f" -> Downloading the testing data of {file_name} into: {directory_path}")

        dataframe.to_csv(f"{directory_path}\\{file_name}_testing_data.csv", index=False)
        print(f" -> Successfully download the testing data of {file_name} into: {directory_path}")


def get_apriori_data():
    print(" -> Start fetching apriori data")

    directory_path = get_data_path()
    filepath = f"{directory_path}\\apriori_data.csv"

    if os.path.isfile(filepath):
        print(" -> Apriori data already exists")

    else:
        start = time.time()
        print(f" -> Querying {db_settings['db']}(database) for apriori data")

        try:
            conn = pymysql.connect(**db_settings)

            with conn.cursor() as cursor:
                # Change your table here
                query = '''
                    SELECT n_id, n_product
                    FROM carrefour_sales;
                    '''

                cursor.execute(query)
                conn.commit()

                result = cursor.fetchall()

                cursor.close()
                conn.close()

        except Exception as ex:
            print(ex)

        end = time.time()

        dataframe = pd.DataFrame(result, columns=["N_Id", "N_Product"])
        print()
        print(dataframe)
        print()

        print(f" -> Spent: {round(end - start, 2)} sec for query")
        print(f" -> Downloading the apriori data into: {directory_path}")

        dataframe.to_csv(f"{directory_path}\\apriori_data.csv", index=False)
        print(f" -> Successfully download the apriori data into: {directory_path}")
