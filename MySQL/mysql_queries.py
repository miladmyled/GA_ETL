import mysql.connector
from config.config import my_sql

input_engine = mysql.connector.connect(host= my_sql['host'],
                                       user= my_sql['user'],
                                       passwd= my_sql['passwd'],
                                       database=my_sql['database'])


def get_fresh_supply_cat(offset):
    cursor = input_engine.cursor()
    query = '''
    select
        digikala.products.id product_id,
        digikala.categories.code,
        digikala.categories.id cat_id,
        digikala.category_tree.parent_id,
        digikala.category_tree.root_id,
        IFNULL(src.title_en,"") supply_cat
    from
        digikala.products
        join digikala.categories on digikala.products.category_id = digikala.categories.id
        join digikala.category_tree on category_tree.category_id = categories.id
		join supply_category_tree on categories.supply_category_id = supply_category_tree.category_id
        join supply_categories src on supply_category_tree.root_id = src.id
    where category_tree.main = 1 and
    src.title_en in ('HC', 'BC', 'PC', 'DF', 'FF');
    '''

    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    result = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return result


def get_main_cats(offset):
    cursor = input_engine.cursor()
    query = '''
    select
        digikala.categories.code,
        digikala.categories.id cat_id
    from
        digikala.categories
        join digikala.category_tree on category_tree.category_id = categories.id
    where category_tree.main = 1 and category_tree.parent_id is NULL;
    '''

    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    result = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return result


def get_old_customers(_date, _limit, _offset):
    cursor = input_engine.cursor()
    query = '''
    SELECT user_id, COUNT(1) as cnt
    FROM digikala.orders
    WHERE status not IN ('canceled', 'canceled_system')
    and DATE(created_at) < "{}"
    GROUP BY user_id 
    limit {} offset {}
    '''.format(_date, _limit, _offset)
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    result = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return result


def get_daily_orders(_date):
    cursor = input_engine.cursor()
    # query = '''
    # SELECT distinct
    # payments.user_id,
    # payments.order_id,
    # payments.status as payment_stat,
    # carts.source as platform,
    # order_items.variant_id,
    # product_variants.product_id,
    # products.category_id,
    # order_shipments.warehouse_id,
    # supply_categories.title_en
    # FROM payments
    # JOIN carts on payments.user_id = carts.user_id
    # JOIN order_items on payments.order_id = order_items.order_id
    # JOIN order_shipments on order_shipments.order_id = payments.order_id
    # JOIN product_variants on product_variants.id = order_items.variant_id
    # join products on products.id = product_variants.product_id
    # join categories on categories.id = products.category_id
    # join category_tree on category_tree.category_id = categories.id
    # join supply_category_tree on categories.supply_category_id = supply_category_tree.category_id
    # join supply_categories on supply_categories.id =  supply_category_tree.root_id
    # WHERE DATE(payments.created_at) = "{}"
    # AND carts.site = 'digikala'
    # AND supply_categories.title_en in ('HC', 'BC', 'PC', 'DF', 'FF')
    # AND order_shipments.warehouse_id in (29, 36)
    # '''.format(_date)
    query = '''
        SELECT distinct
    	payments.user_id,
    	payments.order_id,
    	payments.status as payment_stat,
    	orders.source as platform,
    	order_shipments.warehouse_id
        FROM payments
        JOIN orders on payments.order_id = orders.id
        JOIN order_shipments on payments.order_id = order_shipments.order_id
        WHERE DATE(payments.created_at) = "{}"
		AND order_shipments.warehouse_id in (29, 36) 
        '''.format(_date)
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    result = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return result


def get_closed_orders(_ids):
    cursor = input_engine.cursor()
    # query = '''
    #     SELECT
    # 	orders.id as order_id,
    # 	orders.user_id,
    # 	orders.status,
    # 	orders.source as platform,
    # 	order_shipments.warehouse_id
    #     FROM digikala.orders
    #     JOIN order_shipments on orders.id = order_shipments.order_id
    #     WHERE DATE(orders.updated_at) = "{}"
    #     AND order_shipments.warehouse_id in (29, 36)
    # '''.format(_date)
    query = '''
        SELECT 
    	orders.id as order_id,
    	orders.user_id,
    	orders.status,
    	orders.source as platform
        FROM digikala.orders
        where id in ({})
    '''.format(_ids)
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    result = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return result


def get_shipments(_date):
    cursor = input_engine.cursor()
    query = '''
    SELECT
    	carts.user_id,
    	carts.id as cart_id,
    	carts.status as cart_stat,
    	carts.source as platform,
    	cart_shipments.warehouse_id,
    	cart_shipments.active
    FROM carts
    JOIN cart_shipments on carts.id = cart_shipments.cart_id
    WHERE DATE(carts.created_at) = "{}"
    AND DATE(cart_shipments.created_at) = "{}"
    AND cart_shipments.warehouse_id in (29, 36)
    -- AND cart_shipments.active = 1
    '''.format(_date, _date)
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    result = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return result


def get_add2carts(_date, _limit, _offset):
    cursor = input_engine.cursor()
    query = '''
    SELECT distinct
    	carts.id as cart_id,
    	carts.user_id,
    	carts.status as cart_stat,
    	carts.source as platform,
    	user_addresses.city_id,
    	-- cart_items.variant_id,
    	supply_categories.title_en
    FROM digikala.carts
    JOIN user_addresses on carts.user_id = user_addresses.user_id
    JOIN cart_items on carts.id = cart_items.cart_id
    JOIN product_variants on product_variants.id = cart_items.variant_id
    join products on products.id = product_variants.product_id
    join categories on categories.id = products.category_id
    join category_tree on category_tree.category_id = categories.id
    join supply_category_tree on categories.supply_category_id = supply_category_tree.category_id
    join supply_categories on supply_categories.id =  supply_category_tree.root_id
    WHERE DATE(carts.created_at) = "{}"
    AND supply_categories.title_en in ('HC', 'BC', 'PC', 'DF', 'FF')
    AND user_addresses.city_id in (1698, 1623)
    limit {} offset {};
    '''.format(_date, _limit, _offset)
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    result = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return result
