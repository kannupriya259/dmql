import datetime
from logging import warning
from typing import Container
from numpy import product
import pandas as pd
import psycopg2
import streamlit as st
from configparser import ConfigParser

@st.cache
def get_config(filename="database.ini", section="postgresql"):
    parser = ConfigParser()
    parser.read(filename)
    return {k: v for k, v in parser.items(section)}

# @st.cache
def query_db(sql: str):
    # print(f"Running query_db(): {sql}")

    db_info = get_config()

    # Connect to an existing database
    conn = psycopg2.connect(**db_info)

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Execute a command: this creates a new table
    cur.execute(sql)

    # Obtain data
    data = cur.fetchall()

    column_names = [desc[0] for desc in cur.description]

    # Make the changes to the database persistent
    conn.commit()

    # Close communication with the database
    cur.close()
    conn.close()

    df = pd.DataFrame(data=data, columns=column_names)

    return df

def insert_query(sql: str):
    db_info = get_config()

    # Connect to an existing database
    conn = psycopg2.connect(**db_info)

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # Execute a command: this creates a new table
    cur.execute(sql)

    # Make the changes to the database persistent
    conn.commit()

    # Close communication with the database
    cur.close()
    conn.close()

login = False

st.title('Supermarket Management System')

menu = ["Home","Login"]
choice = st.sidebar.selectbox("Menu",menu)

if choice == "Home":
    st.subheader("Home")

    with st.expander("Sample DB Reading and Testing "):
        sql = "SELECT relname FROM pg_class WHERE relkind='r' AND relname !~ '^(pg_|sql_)';"
        try:
            all_suppliers = query_db(sql)["relname"].tolist()
            table_name = st.selectbox("Choose a table", all_suppliers)
        except:
            st.write("Sorry! Something went wrong with your query, please try again.")

        if table_name:
            f"Display the table"

            sql_table = f"Select * from {table_name};"
            try:
                df = query_db(sql_table)
                st.dataframe(df)
            except: st.write("Sorry! Something went wrong with your query, please try again.")

    st.markdown("## Principles of Database Systems Project")
    st.markdown("##### - By -")
    st.markdown("### Chetan Ingle (cmi8525)")
    st.markdown("### Viswanath Nagarajan (vn2065)")
    
elif choice == "Login":

    with st.expander("Login Credentials", expanded=True):
        
        cred_details = st.columns((1,1))

        with cred_details[0]:
            st.write("""
                For Customers Login: \n
                ##### Username: che.ing \n
                ##### Password: pass123
            """)

        with cred_details[1]:
            st.write("""
                For Admin Login: \n 
                ##### Username: admin \n
                ##### Password: admin \n
            """)

    st.sidebar.subheader("Login Section")
    username = st.sidebar.text_input("User Name")
    password = st.sidebar.text_input("Password",type='password')

    user_id = user_type = 0

    login_sql = f"Select * from Login where login_username = '{username}' and login_password = '{password}';"
    
    if st.sidebar.checkbox("Login"):
        try:
            user_info = query_db(login_sql).loc[0]
            user_id, user_type = (user_info["user_id"],user_info["user_type"])
            login = True

            if login:
                st.sidebar.success("Login Successful")
                if int(user_type) == 1:
                    '## Welcome Admin,'
                    menu1 = [
                        'List of employees and departments',
                        'List of Managers and departments',
                        'List of subdepartments',
                        'List of departments with less than X employees',
                        'Products low on stock / supplies',
                        'Top 5 most-selling products',
                        'Products never bought',
                        'Order details and cash flow'
                    ]
                    choice1 = st.selectbox("Menu", menu1)
                    choice1_num = menu1.index(choice1)
                    
                    if choice1_num == 0:
                        st.subheader("List of employees and departments")
                        with st.expander("List of employees and departments",expanded=True):
                            list_of_emp="""SELECT employees.employee_id AS Employee_id,employees.employee_name Employee_Name,manage_departments.dept_id AS Department_id,manage_departments.dept_name AS Department_name 
                                            FROM works_in,manage_departments,employees 
                                            WHERE works_in.department_id=manage_departments.dept_id 
                                            AND works_in.employee_id=employees.employee_id;"""
                            st.dataframe(query_db(list_of_emp))
                    elif choice1_num == 1:
                        st.subheader("List of Managers and departments")
                        with st.expander("List of Managers and departments",expanded=True):
                            list_of_man="""SELECT employees.employee_id,employees.employee_name AS Manager_Name,manage_departments.dept_id,manage_departments.dept_name 
                                            FROM works_in,manage_departments,employees 
                                            WHERE works_in.department_id=manage_departments.dept_id 
                                            AND works_in.employee_id=employees.employee_id 
                                            AND manage_departments.manager_id=works_in.employee_id;"""
                            st.dataframe(query_db(list_of_man))

                    elif choice1_num == 2:
                        st.subheader("List of subdepartments")
                        with st.expander("List of subdepartments",expanded=True):
                            list_of_sd="""SELECT manage_departments.dept_id AS Department_ID,manage_departments.dept_name AS Department_Name,subdepartments.sub_dept_name AS Subdepartment_Name 
                                            FROM manage_departments,subdepartments WHERE manage_departments.dept_id=subdepartments.dept_id;"""
                            st.dataframe(query_db(list_of_sd))
                            
                    elif choice1_num == 3:
                        with st.expander("list of departments with less than X employees",expanded=True):
                            number_of_emp = st.text_input("Number of employees",value=3)
                            number_of_emp_query = f"""SELECT * from (SELECT works_in.department_id AS DI, manage_departments.dept_name AS DN,COUNT(department_id) AS number_of_emp FROM works_in INNER JOIN manage_departments ON manage_departments.dept_id=works_in.department_id GROUP BY DI,DN ORDER BY number_of_emp ASC)AS t1 where number_of_emp<{number_of_emp}"""
                            st.dataframe(query_db(number_of_emp_query))

                    elif choice1_num == 4:
                        st.subheader("Products low on stock / suppliers")
                        with st.expander("Products low on stock / suppliers",expanded=True):
                            number_of_products = st.text_input("Number of products",value=5)
                            low_products=f"""SELECT store_products.store_id,products_suppliedby.product_id,products_suppliedby.product_name,suppliers.supplier_name,store_products.quantity
                                            FROM products_suppliedby,suppliers,store_products
                                            WHERE products_suppliedby.supplier_id=suppliers.supplier_id
                                            AND store_products.product_id=products_suppliedby.product_id
                                            AND store_products.quantity<{number_of_products};"""
                            st.dataframe(query_db(low_products))
                            
                    elif choice1_num == 5:
                        st.subheader("Top 5 most-selling products")
                        with st.expander("Top 5 most-selling products",expanded=True):
                            top_selling_products="""SELECT ps.product_id, ps.product_name, SUM(cd.quantity) as quantity
                                                    FROM products_suppliedby ps, cart_check_out_payment ccop, cart_details cd 
                                                    WHERE cd.cart_id = ccop.cart_id 
                                                    AND cd.product_id = ps.product_id 
                                                    GROUP BY ps.product_id 
                                                    ORDER BY SUM(cd.quantity) DESC LIMIT 5;"""
                            st.dataframe(query_db(top_selling_products))
                            
                    elif choice1_num == 6:
                        st.subheader("Products never bought")
                        with st.expander("Products never bought",expanded=True):
                            products_never_bought="""SELECT P.product_id, P.product_name 
                                                    FROM products_suppliedby P 
                                                    WHERE P.product_id NOT IN (
                                                        SELECT DISTINCT P.product_id 
                                                        FROM cart_check_out_payment CP, cart_details CD, products_suppliedby P 
                                                        WHERE CP.cart_id = CD.cart_id AND P.product_id = CD.product_id)"""
                            st.dataframe(query_db(products_never_bought))

                    elif choice1_num == 7:
                        st.subheader("Order details and cash flow")
                        with st.expander("Order details and cash flow",expanded=True):
                            timestamp1 = st.date_input("Enter the start date")
                            timestamp2 = st.date_input("Enter the end date")
                            timestamp2 = timestamp2 + datetime.timedelta(days=1)
                            totalorder_income=f"""SELECT COUNT(payment_id) AS number_of_orders,SUM(payment_amount) AS total_income 
                                                FROM cart_check_out_payment 
                                                WHERE payment_datetime >= '{timestamp1}' AND payment_datetime < '{timestamp2}';"""
                            total_income = query_db(totalorder_income)
                            if(total_income.loc[0]["total_income"]):
                                st.table(total_income.style.format({"total_income": "{:.2f}"}))
                            else :
                                st.write("No records found.")
    
                if int(user_type) == 2:
                    customer_details_sql = f"Select customer_name, customer_id from Customers where user_id = {user_id};"
                    customer_details = query_db(customer_details_sql).loc[0]
                    customer_name = customer_details["customer_name"]
                    customer_id = customer_details["customer_id"]
                    
                    f"## Hello {customer_name},"
                    options = ["Create Order","Past Orders"]
                    loginchoice = st.selectbox("Menu",options)

                    if loginchoice == "Create Order":
                        cart_id = 0
                        if st.button("Click here to create a new order!"):
                            try:
                                
                                get_last_cart = """SELECT cart_id FROM cart ORDER BY cart_id DESC LIMIT 1;"""
                                c_id = query_db(get_last_cart).loc[0]["cart_id"]
                                st.write(c_id)

                                if c_id > 0:
                                    cart_id = int(c_id) + 1
                                else : 
                                    cart_id = 1
                                st.write(f"Cart Id: {cart_id}")
                                add_cart_sql = f"Insert into cart(cart_id, customer_id) values ({cart_id}, {customer_id})"
                                insert_query(add_cart_sql)
                                st.write(f"Cart Id: {cart_id} Created.")
                            except:
                                st.write()



                        all_stores_sql = "Select store_address from stores order by store_address;"
                        all_stores = query_db(all_stores_sql)["store_address"].tolist()
                        storeChoice = st.selectbox("Select a Store: ", all_stores)
                        get_store_id_sql = f"Select store_id from stores where store_address = '{storeChoice}';"
                        store_id = query_db(get_store_id_sql).loc[0]["store_id"]


                        all_departments_sql = f"""Select D.dept_name
                                                from store_departments SD, manage_departments D
                                                where SD.store_id = {store_id}
                                                and D.dept_id = SD.dept_id
                                                order by D.dept_name;"""

                        all_departments = query_db(all_departments_sql)["dept_name"].tolist()
                        departmentChoice = st.selectbox("Select a department: ", all_departments)

                        subdepartments_sql = f"""Select S.sub_dept_name 
                                                    from subdepartments S, manage_departments M
                                                    where M.dept_id = S.dept_id
                                                    and M.dept_name = '{departmentChoice}'
                                                    order by S.sub_dept_name; """
                        subdepartments = query_db(subdepartments_sql)["sub_dept_name"].tolist()
                        subdepartmentChoice = st.selectbox("Select a subdepartment:", subdepartments)

                        get_departmentid_sql = f"Select dept_id from manage_departments where dept_name = '{departmentChoice}'"
                        department_id = query_db(get_departmentid_sql).loc[0]["dept_id"]
                        
                        get_subdepartmentid_sql = f"Select sub_dept_id from subdepartments where sub_dept_name = '{subdepartmentChoice}'"
                        subdepartment_id = query_db(get_subdepartmentid_sql).loc[0]["sub_dept_id"]

                        get_products_sql = f"""Select product_name
                                            from products_suppliedby P, store_products SP 
                                            where SP.product_id = P.product_id
                                            and SP.store_id = {store_id}
                                            and P.department_id = {department_id}
                                            and P.sub_department_id = {subdepartment_id} """
                        products = query_db(get_products_sql)
                        productsChoice = st.selectbox("Select a product", products)

                        get_product_details_sql = f"Select product_id, product_name, price, product_description from products_suppliedby where product_name = '{productsChoice}'"
                        product_details = query_db(get_product_details_sql).loc[0]
                        productId, productName, price, description = (product_details["product_id"],product_details["product_name"],
                                                                        product_details["price"],product_details["product_description"])
                        
                        info_columns = st.columns((1,1))

                        with info_columns[0]:
                            st.write(f"* Product Name: {productName}")

                        with info_columns[1]:
                            st.write(f"* Price: ${price}")
                            st.write(f"* Product ID: {productId}")
                        
                        st.write(f"* Description: {description}")

                        qty = int(st.number_input(f"Enter quantity you would like to buy", min_value = 1, max_value = 50, value = 1, step =1))

                        get_last_cart = "SELECT cart_id FROM cart ORDER BY cart_id DESC LIMIT 1;"
                        cart_id = int(query_db(get_last_cart).loc[0]["cart_id"])
                        # st.success(f"{cart_id}, {store_id}, {productId}, {qty}")
                        if st.button("Add to cart"): 
                            save_details_to_cart_sql = f"Insert into cart_details(cart_id, store_id, product_id, quantity) values ({cart_id}, {store_id}, {productId}, {qty});"
                            insert_query(save_details_to_cart_sql)
                            # update_inventory_sql = f"Update store_products set quantity = 90 where store_id = {store_id} and product_id = {productId};"
                            # query_db(update_inventory_sql)
                        
                        get_items_added_sql = f"""Select CD.product_id, P.product_name, CD.quantity, P.price as Unit_Price,P.price*CD.quantity as price
                                                from cart_details CD, products_suppliedby P, cart C
                                                where CD.product_id = P.product_id
                                                and CD.store_id = '{store_id}'
                                                and C.cart_id = CD.cart_id
                                                and C.customer_id = '{customer_id}'
                                                and C.cart_id = {cart_id}"""

                        f"Items added to your cart:"
                        df = query_db(get_items_added_sql)
                        total_bill= pd.Series(df["price"]).sum()
                        st.dataframe(df)
                        f'### Total bill: {total_bill} USD'
                        
                        st.write({cart_id},{customer_id}) 

                        if st.button("Proceed to Checkout & Pay"):
                            date_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            st.write(f"{date_time}")
                            checkout_payment_sql = f"""Insert into Cart_check_out_Payment (payment_datetime, payment_amount, payment_type, cart_id, customer_id) values
                                                    ('{date_time}', {total_bill}, 'card', {cart_id}, {customer_id});"""
                            get_payment_id_sql = f"""Select payment_id from Cart_check_out_Payment 
                                                    where cart_id = '{cart_id}' and customer_id = '{customer_id}'
                                                    ORDER BY payment_id DESC LIMIT 1;"""
                            try:
                                insert_query(checkout_payment_sql)
                                payment_id = int(query_db(get_payment_id_sql).loc[0]["payment_id"])
                                st.write(f"Payment ID: {payment_id}")

                                create_Order_history_sql = f"""Insert into Order_History values ({customer_id},{payment_id});"""
                                insert_query(create_Order_history_sql)
                                st.success("Order has been placed successfully!")

                                cart_id = 0
                                if store_id == 1 or store_id == 2:
                                    store_addr_sql = f"Select store_address from stores where store_id = {store_id}"
                                    address = query_db(store_addr_sql).loc[0]["store_address"]
                                    st.success(f"Your order will be ready for pick up at {address} in 2 Hours.")
                                if store_id == 3:
                                    st.success(f"Your order will be delivered within couple of days.")
                                
                            except:
                                st.write("Sorry! Something went wrong with your query, please try again.")



                    if loginchoice == "Past Orders":
                        get_past_orders_sql = f"""Select O.order_id as Order_ID, P.payment_datetime as date,P.payment_amount as Total 
                                                from Order_History O, cart_check_out_payment P where O.order_id = P.payment_id 
                                                and P.customer_id = {customer_id}
                                                order by O.order_id desc"""
                        past_orders = query_db(get_past_orders_sql)
                        "## Order History:"
                        st.table(past_orders.style.format({"Total": "{:.2f}"}))

                        order_idsList = past_orders["order_id"].tolist()
                        # st.write(f"{order_idsList}")

                        order_id = st.selectbox("Select Order ID",order_idsList)

                        "## Order Details:"
                        order_details_sql = f"""select St.store_address, O.order_id, C.payment_amount, CD.product_id, P.product_name, D.dept_name, S.sub_dept_name, CD.quantity, P.price as unit_price, P.price*CD.quantity as price
                                            from Order_History O, cart_check_out_payment C, cart_details CD, products_suppliedby P, manage_departments D, subdepartments S, Stores St
                                            where C.payment_id = O.order_id
                                            and O.order_id = {order_id}
                                            and C.cart_id = CD.cart_id
                                            and CD.product_id = P.product_id
                                            and D.dept_id = P.department_id
                                            and P.sub_department_id = S.sub_dept_id
                                            and S.dept_id = D.dept_id
                                            and CD.store_id = St.store_id"""
                        order_details = query_db(order_details_sql)
                        orderTotal = order_details.loc[0]["payment_amount"]
                        store_addr = order_details.loc[0]["store_address"]
                        order_details = order_details.drop('payment_amount', 1)
                        order_details = order_details.drop('store_address', 1)
                        f"### Total: {orderTotal}   | Store: {store_addr}"
                        st.dataframe(order_details.style.format({"price": "{:.2f}"}))

                        st.markdown("---")
                        f"## Find Order details between date range:"
                        start = st.date_input("Select start date:")
                        end = st.date_input("Select end date:")
                        end = end + datetime.timedelta(days=1)

                        get_past_orders_inRange_sql = f"""Select O.order_id as Order_ID, P.payment_datetime as date,P.payment_amount as Total 
                                                from Order_History O, cart_check_out_payment P where O.order_id = P.payment_id 
                                                and P.customer_id = {customer_id}
                                                and P.payment_datetime >= '{start}'
                                                and P.payment_datetime < '{end}'
                                                order by O.order_id desc"""
                        past_orders_inRange = query_db(get_past_orders_inRange_sql)
                        f"### Order placed between {start} and {end}:"
                        st.table(past_orders_inRange.style.format({"Total": "{:.2f}"}))

                        order_ids_Range_List = past_orders_inRange["order_id"].tolist()
                        # st.write(f"{order_ids_Range_List}")
                        order_id_Range = st.selectbox("Select Order ID from above",order_ids_Range_List)
                        st.write(order_id_Range)




                        order_details_dateRange_sql = f"""select St.store_address, O.order_id, C.payment_amount, CD.product_id, P.product_name, D.dept_name, S.sub_dept_name, CD.quantity, P.price as unit_price, P.price*CD.quantity as Price
                                                        from Order_History O, cart_check_out_payment C, cart_details CD, products_suppliedby P, manage_departments D, subdepartments S, Stores St
                                                        where C.payment_id = O.order_id
                                                        and O.order_id = {order_id_Range}
                                                        and C.cart_id = CD.cart_id
                                                        and CD.product_id = P.product_id
                                                        and D.dept_id = P.department_id
                                                        and P.sub_department_id = S.sub_dept_id
                                                        and S.dept_id = D.dept_id
                                                        and CD.store_id = St.store_id"""
                        order_details_dateRange = query_db(order_details_dateRange_sql)

                        orderTotalDate = order_details_dateRange.loc[0]["payment_amount"]
                        store_addrDate = order_details_dateRange.loc[0]["store_address"]
                        order_details_dateRange = order_details_dateRange.drop('payment_amount', 1)
                        order_details_dateRange = order_details_dateRange.drop('store_address', 1)
                        f"### Total: {orderTotalDate}   | Store: {store_addrDate}"
                        st.dataframe(order_details_dateRange.style.format({"Price": "{:.2f}"}))
                        
                        st.markdown("---")


                        f"### Your most bought items:"
                        number = int(st.number_input('Enter the number of items you want to see', min_value = 1, value = 1, step =1))

                        f"#### Top {number} products purchased:"
                        top_products_sql = f"""Select PS.product_name, SUM(CD.quantity) AS No_of_times_purchased
                                            from order_history O, cart_check_out_payment P, cart_details CD, Products_suppliedby PS
                                            where O.customer_id =  {customer_id}
                                            and O.order_id = P.payment_id
                                            and P.cart_id = CD.cart_id
                                            and CD.product_id = PS.product_id
                                            group by PS.product_name
                                            order by SUM(CD.quantity) desc
                                            limit {number};"""
                        top_products = query_db(top_products_sql)
                        st.table(top_products)



            else: 
                st.warning("Incorrect username or password")

        except:
            st.write("Sorry! Something went wrong with your query, please try again.")     
