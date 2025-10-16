import sqlite3

def func():
    conn = sqlite3.connect(r'C:\Users\gajananda.mallidi\Downloads\NEWPUSH\NEWPUSH\LLM_Backend\db.sqlite3')
    cursor = conn.cursor()

    # cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 't_86%';")
    cursor.execute("UPDATE t_113_Product_Basic_Data_mandatory SET ATTYP = 'satya' WHERE PRODUCT = 'RM120';")
    # tables = cursor.fetchall()

    # for table in tables:
    #     drop_stmt = f'DROP TABLE IF EXISTS "{table[0]}";'
    #     cursor.execute(drop_stmt)

    conn.commit()
    conn.close()
    print("done")

func()
# # def clear_table(db_path, table_name):
# #     conn = sqlite3.connect(db_path)
# #     cursor = conn.cursor()
# #     try:
# #         rows = cursor.execute(f"""
# #            delete from {table_name};
# #         """)
        
# #         conn.commit()
# #     except Exception as e:
# #         print("SQLite error:", e)
# #     # for i in rows.fetchall():
# #     #     print(i[0])
# #     # conn.commit()
# #     conn.close()
# # # tb=input()    
# # # clear_table(r"C:\Users\gajananda.mallidi\Downloads\NEWPUSH\NEWPUSH\LLM_Backend\db.sqlite3",tb)
# import pandas as pd

# # sheet_index=1
# # skiprows=15
# # skipcols=1
# # file=r"C:\Users\gajananda.mallidi\Downloads\NEWPUSH\NEWPUSH\LLM_Backend\connection\SAP_Inventory_Accelerators.xlsx"
# # df = pd.read_excel(file, sheet_name=sheet_index, skiprows=skiprows, usecols=lambda c: True)
# # df = df.iloc[:, skipcols:]

# # df.to_excel('output.xlsx', index=False)




# import pandas as pd

# def dynamic_side_by_side_select(db_path, table_a, table_b, join_columns):
#     import sqlite3
#     conn = sqlite3.connect(db_path)
#     cur = conn.cursor()
#     cur.execute(f"PRAGMA table_info({table_a})")
#     columns_info = cur.fetchall()
#     columns = [col[1] for col in columns_info]

#     select_list = []
#     col_names = []
#     for col in columns:
#         select_list.append(f"src.{col}")
#         col_names.append(f"src_{col}")
#         select_list.append(f"trans.{col}")
#         col_names.append(f"trans_{col}")
#     select_clause = ", ".join(select_list)

#     join_conditions = " AND ".join([f"src.{col} = trans.{col}" for col in join_columns])

#     query = f"""
#     SELECT {select_clause}
#     FROM {table_a} src
#     INNER JOIN {table_b} trans
#     ON {join_conditions}
#     """

#     cur.execute(query)
#     rows = cur.fetchall()
#     conn.close()

#     # Convert to pandas DataFrame with alternated column names for clarity
#     df = pd.DataFrame(rows, columns=col_names)
#     return df

# # Usage example
db_path = r"C:\Users\gajananda.mallidi\Downloads\NEWPUSH\NEWPUSH\LLM_Backend\db.sqlite3"
# df = dynamic_side_by_side_select(db_path, 't_100_product_basic_data_mandatory', 't_100_product_basic_data_mandatory_src', ['PRODUCT'])

# json_data = df.to_dict(orient='records')
# # return JsonResponse(json_data, safe=False)



# import sqlite3
# import pandas as pd

# def create_and_validate_test_tables(db_path):
#     conn = sqlite3.connect(db_path)
#     cur = conn.cursor()

#     # Create tables including 'reason' in errortable
#     cur.executescript("""
#         DROP TABLE IF EXISTS datatable;
#         DROP TABLE IF EXISTS errortable;
#         CREATE TABLE datatable (
#             id INTEGER,
#             code TEXT,
#             value TEXT
#         );
#         CREATE TABLE errortable (
#             id INTEGER,
#             code TEXT,
#             value TEXT,
#             reason TEXT
#         );
#     """)
#     conn.commit()

#     # Insert test data
#     test_records = [
#         (1, 'A', 'X'),
#         (2, 'B', 'Y'),
#         (None, 'C', 'Z'),
#         (3, None, 'W'),
#         (2, 'B', 'Y'),
#         (4, 'D', 'Q'),
#         (2, 'B', 'Z'),
#         (2, 'E', 'Y'),
#     ]
#     cur.executemany("INSERT INTO datatable (id, code, value) VALUES (?, ?, ?);", test_records)
#     conn.commit()

#     # Load data into DataFrame
#     df = pd.read_sql_query("SELECT * FROM datatable", conn)
#     pk_fields = ['id', 'code']

#     error_df = pd.DataFrame(columns=list(df.columns) + ['reason'])

#     def build_reason(issue, row):
#         keys = ", ".join([f"{k}={row[k]}" for k in pk_fields])
#         return f"{issue} ({keys})"

#     # Null in PK
#     null_pk_mask = df[pk_fields].isnull().any(axis=1)
#     null_rows = df[null_pk_mask].copy()
#     null_rows['reason'] = null_rows.apply(lambda row: build_reason("Null in PK", row), axis=1)
#     error_df = pd.concat([error_df, null_rows])

#     # Duplicate PK (excluding null PKs)
#     not_null_mask = ~df[pk_fields].isnull().any(axis=1)
#     dup_pk_mask = df[not_null_mask].duplicated(subset=pk_fields, keep=False)
#     dup_pk_rows = df[not_null_mask][dup_pk_mask].copy()
#     dup_pk_rows['reason'] = dup_pk_rows.apply(lambda row: build_reason("Duplicate PK", row), axis=1)
#     error_df = pd.concat([error_df, dup_pk_rows])

#     # Full row duplicates
#     full_dup_mask = df.duplicated(keep=False)
#     full_dup_rows = df[full_dup_mask].copy()
#     full_dup_rows['reason'] = full_dup_rows.apply(lambda row: build_reason("Full row duplicate", row), axis=1)
#     error_df = pd.concat([error_df, full_dup_rows])

#     error_df = error_df.drop_duplicates()

#     # Save to errortable
#     if not error_df.empty:
#         error_df.to_sql('errortable', conn, if_exists='append', index=False)

#     print(f"Inserted {len(test_records)} rows into datatable.")
#     print(f"Copied {len(error_df)} error rows into errortable.")
#     print("Error rows:")
#     print(error_df)

#     conn.close()

# # Example call:
# create_and_validate_test_tables(r"C:\Users\gajananda.mallidi\Downloads\db.sqlite3")




# import sqlite3

# def duplicate_first_five_records(db_path, table_name):
#     conn = sqlite3.connect(db_path)
#     cur = conn.cursor()

#     # Fetch the first 5 records
#     cur.execute(f"SELECT * FROM {table_name} LIMIT 5")
#     rows = cur.fetchall()

#     if not rows:
#         print("No rows to duplicate.")
#         conn.close()
#         return

#     # Get column names
#     cur.execute(f"PRAGMA table_info({table_name})")
#     columns_info = cur.fetchall()
#     columns = [col[1] for col in columns_info]

#     # Build insert SQL
#     placeholders = ", ".join(["?"] * len(columns))
#     insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

#     # Insert the fetched rows again
#     cur.executemany(insert_sql, rows)
#     conn.commit()
#     print(f"Duplicated {len(rows)} rows into {table_name}.")
#     conn.close()

# # Example usage:
# duplicate_first_five_records(db_path, "t_113_Product_Basic_Data_mandatory")


# from fuzzywuzzy import fuzz

# str1 = 'My name is Satya'
# str2 = 'Hii Iam Satya'

# ratio = fuzz.ratio(str1, str2)
# print(ratio)

# from hdbcli import dbapi

# def HANAconn():
#     print("Hana")
#     try:
#         conn = dbapi.connect(
#             address="10.56.7.40",
#             port=30015,
#             user = 'SATYAR',
#             password= 'SunR!se@6789',
#             encrypt='true',
#             sslValidateCertificate='false'
#         )
#         print(conn.isconnected())
        
#         cursor = conn.cursor()
#         cursor.execute("")
#         rows = cursor.fetchall()
#         rows=list(rows)
 
#         print(rows)
#     #     # return Response(tables)
       
#     #     return Response(tables,status=status.HTTP_200_OK)
#     except Exception as e:
#         print(e)
#     if(conn.isconnected):  
#         print("connected")
#         return 
#     print("not connected")


# HANAconn()

# from datetime import datetime

# import sqlite3

# def copy_rows_with_reason_and_timestamp(table1, table2, row_indices, prompt):
#     import pandas as pd

#     # Load table1 into DataFrame using raw connection for full compatibility
#     conn = sqlite3.connect(db_path)
#     df1 = pd.read_sql_query(f"SELECT * FROM {table1}", conn)
    
#     if not row_indices:
#         print("No row indices provided to copy.")
#         return
    
#     # Get the rows by index
#     try:
#         selected_df = df1.iloc[row_indices].copy()
#     except Exception as e:
#         print(f"Error selecting rows: {e}")
#         return

#     # Add the extra columns
#     selected_df['prompt'] = prompt
#     selected_df['last_updated_on'] = datetime.now()  # or .isoformat()
#     print(selected_df.head(5))
#     # Write to table2 (assume table2 already exists with correct schema)
#     selected_df.to_sql(table2, conn, if_exists='append', index=False)
#     print(f"Inserted {len(selected_df)} rows into {table2} with reason and last_updated_on.")

# # Example usage:
# copy_rows_with_reason_and_timestamp("t_112_Product_Basic_Data_mandatory", 
#                                     't_112_Product_Basic_Data_mandatory_val', 
#                                     [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 
# 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 36, 137,  144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 
# 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 250, 351, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 396, 397, 398, 399, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411]
# , "selection cretria : Filter and update based on Material type = ROH")
# print("success")

