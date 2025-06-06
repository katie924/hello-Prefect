import pandas as pd

from lib.utils import connect_post, pd_append_sql, website_boolean, assign_age_group

rename_order = {'order_id': 'orders'}
idx_web = ['date', 'website_name']
idx_online = ['date', 'is_online']


class BasicData:
    def __init__(self, con):
        """
        Initialize the BasicData class with a database connection.
        Loads all necessary datasets during initialization.

        Args:
            con: SQLAlchemy Connection object used to interact with the database.
        """

        self.order_data = self.load_order_data(con)
        self.member_data = self.load_member_data(con)
        self.purchase_data = self.load_purchase_data(con)

    def load_order_data(self, con) -> pd.DataFrame:
        sql = '''
        SELECT * FROM info.order_record;
        '''
        return pd.read_sql(sql, con, parse_dates=["date"])

    def load_member_data(self, con) -> pd.DataFrame:
        sql = '''
        SELECT member_id, city, region, register_date AS date, gender, birth_date, website_name
        FROM info.member_info
        WHERE is_member = true;
        '''
        return pd.read_sql(sql, con, parse_dates=["date", "birth_date"])

    def load_purchase_data(self, con) -> pd.DataFrame:
        sql = '''
        SELECT p.*, b.brand_id, b.category_id, b.business_id
        FROM info.purchase_record AS p
        LEFT JOIN info.product_info AS b ON p.product_id = b.product_id;
        '''
        return pd.read_sql(sql, con, parse_dates=["date"])


def basic_metric(df: pd.DataFrame, idx: list, agg_type: str = 'all') -> pd.DataFrame:
    """
    Perform aggregation on the DataFrame based on the specified grouping keys and aggregation type.

    Args:
        df (pd.DataFrame): Input DataFrame.
        idx (list): Columns to group by.
        agg_type (str): Aggregation type. Default is 'all'.
            - 'all': Aggregate {'revenue': 'sum', 'order_id': 'count'}, and rename 'order_id' to 'orders'.
            - 'revenue': Aggregate revenue only.

    Returns:
        pd.DataFrame: Aggregated result based on the `agg_type`:
            - If agg_type == 'all': idx, 'revenue', 'orders'
            - If agg_type == 'revenue': idx, 'revenue'
    """
    df_group = df_ = df.groupby(idx, as_index=False)
    if agg_type == 'all':
        df_ = df_group.agg({'revenue': 'sum', 'order_id': 'count'})\
            .rename(columns=rename_order)
    elif agg_type == 'revenue':
        df_ = df_group['revenue'].sum()
    return df_


def revenue_overview(con, order_data, member_data):
    # 營業額、訂單數
    df = order_data[idx_web + ['is_member', 'revenue', 'order_id']]
    df_rm = basic_metric(df.query("is_member == True"), idx_web)
    df_rnm = basic_metric(df.query("is_member == False"), idx_web)
    df_r = pd.merge(df_rm, df_rnm, on=idx_web, how='outer')\
        .rename(columns={'revenue_x': 'member_revenue', 'orders_x': 'member_orders',
                         'revenue_y': 'not_member_revenue', 'orders_y': 'not_member_orders'})

    # 會員數
    df_m = member_data.groupby(idx_web)['member_id'].agg('count')\
        .reset_index().rename(columns={'member_id': 'new_member_count'})
    df_final = df_r.merge(df_m, on=idx_web, how='outer').sort_values(idx_web).fillna(0)
    df_final['member_count'] = df_final.groupby('website_name')['new_member_count'].cumsum()

    pd_append_sql(con, df_final, "revenue_overview", schema="metric")


def region_revenue(con, order_data_member, member_data):
    df = website_boolean(order_data_member)[
        idx_online + ['city', 'region', 'revenue', 'order_id', 'member_id']]
    idx_city = idx_online + ['city', 'region']

    # 收貨地區分布: 營業額、訂單數
    df_go = basic_metric(df, idx_city)
    df_go['type'] = 'shipping'

    # 購買人地區: 營業額、訂單數
    df_gm = df.drop(columns=['city', 'region'])\
        .merge(member_data.drop(columns='date'), on='member_id')
    df_gm = basic_metric(df_gm, idx_city)
    df_gm['type'] = 'member'

    df_final = pd.concat([df_go, df_gm])
    pd_append_sql(con, df_final, "region_revenue", schema="metric")


def source_revenue(con, order_data_member, order_data):
    # 線上會員來源: 營業額
    df = order_data_member.query('website_name != "0"').assign(is_online=True)
    df1 = basic_metric(df, idx_online + ['source'], agg_type='revenue')

    # 線下會員/非會員: 營業額
    df = order_data.query('website_name == "0"').assign(
        source=lambda d: d['is_member'].map({True: '會員', False: '非會員'}),
        is_online=False
    )
    df2 = basic_metric(df, idx_online + ['source'], agg_type='revenue')

    df_final = pd.concat([df1, df2])
    pd_append_sql(con, df_final, "source_revenue", schema="metric")


def store_revenue(con, order_data):
    # 門市: 營業額
    df = order_data.query('website_name == "0"')
    df_final = basic_metric(df, ['date', 'source'], agg_type='revenue')\
        .rename(columns={'source': 'store_id'})

    pd_append_sql(con, df_final, "store_revenue", schema="metric")


def hourly_revenue_store(con, order_data):
    # 門市小時: 營業額
    df = order_data.query('website_name == "0"').copy()
    df['hour'] = df['time'].apply(lambda t: t.hour)
    df_final = basic_metric(df, ['date', 'hour'], agg_type='revenue')

    pd_append_sql(con, df_final, "hourly_revenue_store", schema="metric")


def hourly_revenue(con, order_data_member):
    # 小時: 營業額、訂單數
    df = order_data_member.copy()[idx_web + ['time', 'revenue', 'order_id']]
    df['hour'] = df['time'].apply(lambda t: t.hour)
    df_final = basic_metric(df, idx_web + ['hour'])

    pd_append_sql(con, df_final, "hourly_revenue", schema="metric")


def member_revenue_info(con, order_data_member, member_data):
    # 合併會員資料，紀錄消費當下【性別、是否為新會員、年齡分組】
    df_info = website_boolean(order_data_member).merge(
        member_data, on='member_id', how='left', suffixes=('', '_join'))
    df_info['is_new_member'] = (df_info['date'] - df_info['date_join']).dt.days <= 30
    df_info['age'] = ((df_info['date'] - df_info['birth_date']
                       ).dt.days / 365).round()
    df_info = assign_age_group(df_info)
    df_info['age_group'] = df_info['age'].astype(str)

    # 會員: 營業額、訂單數
    df_final = basic_metric(df_info, idx_online + [
        'member_id', 'gender', 'age_group', 'is_new_member'])
    df_final['age_group'] = df_final['age_group'].replace('nan', pd.NA)

    pd_append_sql(con, df_final, "member_revenue_info", schema="metric")


def member_order_interval(con, order_data_member):
    df = order_data_member.copy()[idx_web + ['member_id', 'order_id']]
    df.sort_values(by=['member_id', 'date'], inplace=True)
    # 訂單: 間隔天數
    df['prev_date'] = df.groupby('member_id')['date'].shift()
    df['interval_all'] = (df['date'] - df['prev_date']).dt.days
    # 線上/線下訂單: 間隔天數
    df['prev_date'] = df.groupby(['member_id', 'website_name'])['date'].shift()
    df['interval'] = (df['date'] - df['prev_date']).dt.days
    df_final = df.drop(columns=['order_id', 'prev_date'])\
        .sort_values(by=['date', 'member_id'])

    pd_append_sql(con, df_final, "member_order_interval", schema="metric")


def daily_members(con, order_data_member, member_data):
    # 註冊會員 list
    df_m = member_data\
        .groupby(idx_web).agg({'member_id': list}).reset_index()
    # 有消費會員 list
    df_c = order_data_member\
        .groupby(idx_web).agg({'member_id': list}).reset_index()
    # 會員數
    sql = '''SELECT date, website_name, member_count FROM metric.revenue_overview;'''
    df_revenue = pd.read_sql(sql, con, parse_dates='date')

    df_final = df_m.merge(df_c, on=idx_web, how='outer')\
        .merge(df_revenue, on=idx_web, how='outer')

    # 轉格式 EX: '[]' or '[1, 2, 3]'
    def json_list(x):
        return str(x if isinstance(x, list) else [])
    df_final['registered_members'] = df_final['member_id_x'].apply(json_list)
    df_final['consumed_members'] = df_final['member_id_y'].apply(json_list)
    df_final.drop(columns=['member_id_x', 'member_id_y'], inplace=True)

    pd_append_sql(con, df_final, "daily_members", schema="metric")


def product_sales(con, purchase_data):
    # {item}_sales# & {item}_concurrent
    from itertools import combinations
    df = website_boolean(purchase_data)

    def item_sales(item):
        _id = f'{item}_id'
        # 產品: 銷售額、訂單數、消費人數
        df_sales = df.groupby(idx_online + [_id])\
            .agg({'sales': 'sum', 'order_id': 'nunique', 'member_id': 'nunique'})\
            .reset_index().rename(columns={**rename_order, 'member_id': 'buyers_count'})

        # 同筆訂單同時購買產品
        df_ag = df.groupby(idx_online + ['order_id'])\
            .agg(list=(_id, lambda x: sorted(set(x))), count=(_id, 'nunique'))\
            .reset_index().query('count > 1')
        expanded_rows = []
        for _, row in df_ag.iterrows():
            for combo in combinations(row['list'], 2):
                expanded_rows.append({
                    'date': row['date'],
                    f'{item}_1': combo[0],
                    f'{item}_2': combo[1],
                    'is_online': row['is_online'],
                    'orders': 1
                })

        expanded_df = pd.DataFrame(expanded_rows)
        df_concurrent = expanded_df.groupby(idx_online + [f'{item}_1', f'{item}_2'])\
            .agg('sum').reset_index()

        return df_sales, df_concurrent

    sql = '''SELECT item FROM info.product_item;'''
    items = pd.read_sql(sql, con)['item']
    for item in list(items):
        df_sales, df_concurrent = item_sales(item)

        pd_append_sql(con, df_sales, f"{item}_sales", schema="metric")
        pd_append_sql(con, df_concurrent, f"{item}_concurrent", schema="metric")


def product_group(con, purchase_data):
    def product_sales_sum(df, index_="product_id"):
        return df.groupby(index_, as_index=False)["sales"].sum()

    df_trans = product_sales_sum(purchase_data[["product_id", "date", "sales"]],
                                 ["product_id", "date"])
    latest_date = max(df_trans['date'])
    df_trans = df_trans[df_trans['date'] > (latest_date - pd.DateOffset(years=1))]

    # 未購買商品：一年內未被購買的商品
    sql = '''SELECT product_id FROM info.product_info;'''
    product_data = pd.read_sql(sql, con)
    non_buy = product_data[~product_data["product_id"].isin(df_trans["product_id"])
                           ]["product_id"].unique()

    # 熱銷商品：一年內此商品總消費金額佔整體的前 20%
    hot_trans = product_sales_sum(df_trans)\
        .sort_values("sales", ascending=False)
    hot_trans = hot_trans.head(int(len(hot_trans) / 5))
    hot_buy = hot_trans["product_id"].unique()

    # 成長商品：近 90 天相較於上期消費額成長超過 50%
    date_90 = latest_date - pd.DateOffset(days=90)
    grow_trans = product_sales_sum(df_trans[
        df_trans["date"] > date_90
    ])  # 近 90 天銷售

    grow_trans_prev = product_sales_sum(df_trans[
        (df_trans["date"] > (date_90 - pd.DateOffset(days=90))) &
        (df_trans["date"] <= date_90)
    ])  # 前一期（90 ~ 180天前）的銷售

    grow_trans = grow_trans_prev.merge(
        grow_trans, on="product_id", how="outer").fillna(1)
    grow_trans = grow_trans[grow_trans["sales_y"] /
                            grow_trans["sales_x"] >= 1.5]
    grow_buy = grow_trans["product_id"].unique()

    label_map = {}
    for pid in non_buy:
        label_map[pid] = "未購買"
    for pid in hot_buy:
        label_map.setdefault(pid, "熱銷")
    for pid in grow_buy:
        label_map.setdefault(pid, "成長")

    product_data["group"] = product_data["product_id"].map(
        label_map).fillna("小眾")

    pd_append_sql(con, product_data, "product_group", schema="metric")


def main(tenant_id):
    engine = connect_post(tenant_id)
    with engine.begin() as con:
        bd = BasicData(con)
        order_data = bd.order_data
        order_data_member = order_data.query("is_member == True")
        member_data = bd.member_data
        purchase_data = bd.purchase_data

        revenue_overview(con, order_data, member_data)
        region_revenue(con, order_data_member, member_data)
        source_revenue(con, order_data_member, order_data)
        store_revenue(con, order_data)
        hourly_revenue_store(con, order_data)
        hourly_revenue(con, order_data_member)
        member_revenue_info(con, order_data_member, member_data)
        member_order_interval(con, order_data_member)
        daily_members(con, order_data_member, member_data)
        product_sales(con, purchase_data)
        product_group(con, purchase_data)


if __name__ == "__main__":
    main()
