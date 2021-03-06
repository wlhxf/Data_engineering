import json
import os
import pyecharts
from matplotlib import ticker
import pyecharts.options as opts
from pyecharts.charts import Bar, Grid, Line, Liquid, Page, Pie
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['font.serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False

import seaborn as sns
import warnings
# 设置为seaborn绘图风格
sns.set(style="darkgrid",font_scale=1.5)

sc = SparkContext('local', 'taobao')
sc.setLogLevel('WARN')
spark = SparkSession.builder.getOrCreate()

df = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('taobao_clean.csv')
#print(df.count())
#df = df.sort(['date', 'hour', 'user_id', 'item_id'], ascending=True)
#df.show()
df.createOrReplaceTempView("data")

def _map_to_pandas(rdds):
    """ Needs to be here due to pickling issues """
    return [pd.DataFrame(list(rdds))]
def to_Pandas(df, n_partitions=None):
    """
    Returns the contents of `df` as a local `pandas.DataFrame` in a speedy fashion. The DataFrame is
    repartitioned if `n_partitions` is passed.
    :param df:              pyspark.sql.DataFrame
    :param n_partitions:    int or None
    :return:                pandas.DataFrame
    """
    if n_partitions is not None: df = df.repartition(n_partitions)
    df_pand = df.rdd.mapPartitions(_map_to_pandas).collect()
    df_pand = pd.concat(df_pand)
    df_pand.columns = df.columns
    return df_pand

df = to_Pandas(df)

def pvanduv():
    # total_pv = spark.sql("SELECT COUNT(user_id) AS total_pv FROM data")
    # total_uv = spark.sql("SELECT COUNT(DISTINCT user_id) AS total_uv FROM data")
    # print(total_pv, total_uv)

    # pv_daily = spark.sql("SELECT date, COUNT(user_id) AS uv_daily FROM data GROUP BY date ORDER BY date LIMIT 10")
    # uv_daily = spark.sql("SELECT date, COUNT(DISTINCT user_id) AS uv_daily FROM data GROUP BY date ORDER BY date LIMIT 10")

    pv_daily = df.groupby("date")['user_id'].count()
    uv_daily = df.groupby("date")['user_id'].apply(lambda x: x.nunique())
    # uv_daily = df.groupby("date")['user_id'].apply(lambda x: x.drop_duplicates().count())
    pv_uv_daily = pd.concat([pv_daily, uv_daily], axis=1)
    pv_uv_daily.columns = ["pv", "uv"]
    print(pv_uv_daily.head())

    pv_daily_list = pv_daily.reset_index(name="pv")['pv'].tolist()
    uv_daily_list = uv_daily.reset_index(name="uv")['uv'].tolist()
    minpv = min(pv_daily_list)
    maxpv = max(pv_daily_list)
    minuv = min(uv_daily_list)
    maxuv = max(uv_daily_list)
    c = (
        Line(init_opts=opts.InitOpts(width="1800px", height="800px"))
            .add_xaxis(pv_daily.reset_index(name="pv")['date'].tolist()).add_yaxis(
            "每天页面的总访问量(PV)",
            pv_daily_list,
            yaxis_index=0,
            markline_opts=opts.MarkLineOpts(data=[opts.MarkLineItem(type_="average")]),
        )
        .add_yaxis(
            "每天页面的独立访客数(UV)",
            uv_daily_list,
            yaxis_index=1,
            markline_opts=opts.MarkLineOpts(data=[opts.MarkLineItem(type_="average")]),
        )
        .extend_axis(yaxis=opts.AxisOpts(name='每天页面的独立访客数(UV)', type_='value', position="right", min_=minuv, offset=80))
        .set_global_opts(title_opts=opts.TitleOpts(title="PV和UV的变化趋势"),
                         tooltip_opts=opts.TooltipOpts(trigger="axis"),
                         toolbox_opts=opts.ToolboxOpts(is_show=True),
                         xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False),
                         yaxis_opts=opts.AxisOpts(type_='value', name='每天页面的总访问量(PV)', min_=minpv, offset=40)
                         )
        .render("pvanduv.html")
    )


    # 绘图代码如下
    # plt.figure(figsize=(16, 10))
    # plt.subplot(211)
    # plt.plot(pv_daily, c="r")
    # plt.title("每天页面的总访问量(PV)", fontsize=20)
    # plt.subplot(212)
    # plt.plot(uv_daily, c="g")
    # plt.title("每天页面的独立访客数(UV)", fontsize=20)
    # # plt.suptitle("PV和UV的变化趋势")
    # plt.tight_layout()
    # plt.savefig("PV和UV的变化趋势", dpi=300)
    # plt.show()


def process1():
    # pv_hour = spark.sql("SELECT hour, COUNT(user_id) AS pv_hour FROM data GROUP BY hour ORDER BY hour")
    # uv_hour = spark.sql("SELECT hour, COUNT(DISTINCT user_id) AS uv_hour FROM data GROUP BY hour ORDER BY hour")

    pv_hour = df.groupby("hour")['user_id'].count()
    pv_hour.head()
    uv_hour = df.groupby("hour")['user_id'].apply(lambda x: x.nunique())
    uv_hour.head()
    pv_uv_hour = pd.concat([pv_hour, uv_hour], axis=1)
    pv_uv_hour.columns = ["pv_hour", "uv_hour"]
    pv_uv_hour.head()
    #绘图代码如下

    pv_hour_list = pv_hour.reset_index(name="pv")['pv'].tolist()
    uv_hour_list = uv_hour.reset_index(name="uv")['uv'].tolist()
    minpv = min(pv_hour_list)
    minuv = min(uv_hour_list)
    c = (
        Line(init_opts=opts.InitOpts(width="1800px", height="800px"))
            .add_xaxis(pv_hour.reset_index(name="pv")['hour'].tolist()).add_yaxis(
            "每个小时的页面总访问量",
            pv_hour_list,
            yaxis_index=0,
            markline_opts=opts.MarkLineOpts(data=[opts.MarkLineItem(type_="average")]),
            is_smooth=True,
        )
            .add_yaxis(
            "每个小时的页面独立访客数",
            uv_hour_list,
            yaxis_index=1,
            markline_opts=opts.MarkLineOpts(data=[opts.MarkLineItem(type_="average")]),
            is_smooth=True,
        )
            .extend_axis(
            yaxis=opts.AxisOpts(name='每个小时的页面独立访客数', type_='value', position="right", min_=minuv, offset=80))
            .set_global_opts(title_opts=opts.TitleOpts(title="每个小时的PV和UV的变化趋势"),
                             tooltip_opts=opts.TooltipOpts(trigger="axis"),
                             toolbox_opts=opts.ToolboxOpts(is_show=True),
                             xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False),
                             yaxis_opts=opts.AxisOpts(type_='value', name='每个小时的页面总访问量', min_=minpv, offset=40)
                             )
            .render("pvanduvhour.html")
    )

    # plt.figure(figsize=(16, 10))
    # pv_uv_hour["pv_hour"].plot(c="steelblue", label="每个小时的页面总访问量", fontsize=20)
    # plt.ylabel("页面访问量", fontsize=20)
    #
    # pv_uv_hour["uv_hour"].plot(c="red", label="每个小时的页面独立访客数", secondary_y=True, fontsize=20)
    # plt.ylabel("页面独立访客数", fontsize=20)
    # plt.xticks(range(0, 24), pv_uv_hour.index)
    #
    # plt.legend(loc="best")
    # plt.grid(True)
    #
    # plt.tight_layout()
    # plt.savefig("每个小时的PV和UV的变化趋势", dpi=300)
    # plt.show()


def process2():
    # type_1 = df[df['behavior_type'] == "1"]["user_id"].count()
    # type_2 = df[df['behavior_type'] == "2"]["user_id"].count()
    # type_3 = df[df['behavior_type'] == "3"]["user_id"].count()
    # type_4 = df[df['behavior_type'] == "4"]["user_id"].count()
    type_1 = spark.sql("SELECT COUNT(user_id) FROM data where behavior_type = '1'")
    type_1 = to_Pandas(type_1)['count(user_id)'][0]
    type_2 = spark.sql("SELECT COUNT(user_id) FROM data where behavior_type = '2'")
    type_2 = to_Pandas(type_2)['count(user_id)'][0]
    type_3 = spark.sql("SELECT COUNT(user_id) FROM data where behavior_type = '3'")
    type_3 = to_Pandas(type_3)['count(user_id)'][0]
    type_4 = spark.sql("SELECT COUNT(user_id) FROM data where behavior_type = '4'")
    type_4 = to_Pandas(type_4)['count(user_id)'][0]
    print("点击用户：", type_1)
    print("收藏用户：", type_2)
    print("添加购物车用户：", type_3)
    print("支付用户：", type_4)

    # pv_date_type = df.groupBy("date").pivot("behavior_type").sum("user_id").show()
    pv_date_type = pd.pivot_table(df, index='date', columns='behavior_type', values='user_id', aggfunc=np.size)
    pv_date_type.columns = ["点击", "收藏", "加入购物车", "支付"]
    # 绘图如下
    c = (
        Line(init_opts=opts.InitOpts(width="1800px", height="800px"))
            .add_xaxis(pv_date_type.reset_index()['date'].tolist())
            .add_yaxis(
            "收藏",
            pv_date_type.reset_index()['收藏'].tolist(),
            yaxis_index=0,
        )
            .add_yaxis(
            "加入购物车",
            pv_date_type.reset_index()['加入购物车'].tolist(),
            yaxis_index=0,
        )
            .add_yaxis(
            "支付",
            pv_date_type.reset_index()['支付'].tolist(),
            yaxis_index=0,
        )
            .add_yaxis(
            "点击",
            pv_date_type.reset_index()['点击'].tolist(),
            yaxis_index=1,
        )
            .extend_axis(yaxis=opts.AxisOpts(name='点击次数', type_='value', position="right", offset=40))
            .set_global_opts(title_opts=opts.TitleOpts(title="不同日期不同用户行为的PV变化趋势"),
                             tooltip_opts=opts.TooltipOpts(trigger="axis"),
                             toolbox_opts=opts.ToolboxOpts(is_show=True),
                             xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=True),
                             yaxis_opts=opts.AxisOpts(type_='value', name='次数', offset=20)
                             )
            .render("userbydate.html")
    )

    # plt.figure(figsize=(16, 10))
    # sns.lineplot(data=pv_date_type[['收藏', '加入购物车', '支付']])
    #
    # plt.tight_layout()
    # plt.savefig("不同日期不同用户行为的PV变化趋势", dpi=300)
    # plt.show()


    # pv_hour_type = df.groupBy("hour").pivot("behavior_type").sum("user_id").show()
    pv_hour_type = pd.pivot_table(df, index='hour', columns='behavior_type', values='user_id', aggfunc=np.size)
    pv_hour_type.columns = ["点击", "收藏", "加入购物车", "支付"]

    # 绘图如下

    c = (
        Line(init_opts=opts.InitOpts(width="1800px", height="800px"))
            .add_xaxis(pv_hour_type.reset_index()['hour'].tolist())
            .add_yaxis(
            "收藏",
            pv_hour_type.reset_index()['收藏'].tolist(),
            yaxis_index=0,
        )
            .add_yaxis(
            "加入购物车",
            pv_hour_type.reset_index()['加入购物车'].tolist(),
            yaxis_index=0,
        )
            .add_yaxis(
            "支付",
            pv_hour_type.reset_index()['支付'].tolist(),
            yaxis_index=0,
        )
            .add_yaxis(
            "点击",
            pv_hour_type.reset_index()['点击'].tolist(),
            yaxis_index=1,
        )
            .extend_axis(yaxis=opts.AxisOpts(name='点击次数', type_='value', position="right", offset=40))
            .set_global_opts(title_opts=opts.TitleOpts(title="不同小时不同用户行为的PV变化趋势"),
                             tooltip_opts=opts.TooltipOpts(trigger="axis"),
                             toolbox_opts=opts.ToolboxOpts(is_show=True),
                             xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=True),
                             yaxis_opts=opts.AxisOpts(type_='value', name='次数', offset=20)
                             )
            .render("userbyhour.html")
    )

    # # plt.figure(figsize=(16, 10))
    # # sns.lineplot(data=pv_hour_type[['收藏', '加入购物车', '支付']])
    #
    # # pv_hour_type["点击"].plot(c="pink", linewidth=5, label="点击", secondary_y=True)
    # # plt.legend(loc="best")
    # #
    # # plt.tight_layout()
    # # plt.savefig("不同小时不同用户行为的PV变化趋势", dpi=300)
    # # plt.show()

    # 支付次数前10的用户行为细分

    # buy_first = df.groupBy("user_id").pivot("behavior_type").sum("user_id").show()
    # buy_first_10 = spark.sql("SELECT count(behavior_type) as cb FROM data where behavior_type = '3' ORDER BY cb DESC LIMIT 10")
    df["user_id1"] = df["user_id"]
    buy_first = pd.pivot_table(df, index='user_id', columns='behavior_type', values='user_id1', aggfunc="count")
    buy_first.columns = ["点击", "收藏", "加入购物车", "支付"]
    buy_first_10 = buy_first.sort_values(by='支付', ascending=False)[:10].astype('int').reset_index().astype('string')

    print(buy_first_10)
    # 绘制图形如下
    c = (
        Line(init_opts=opts.InitOpts(width="1800px", height="800px"))
            .add_xaxis(buy_first_10['user_id'].tolist())
            .add_yaxis(
            "收藏",
            buy_first_10['收藏'].tolist(),
            yaxis_index=0,
        )
            .add_yaxis(
            "加入购物车",
            buy_first_10['加入购物车'].tolist(),
            yaxis_index=0,
        )
            .add_yaxis(
            "支付",
            buy_first_10['支付'].tolist(),
            yaxis_index=0,
        )
            .add_yaxis(
            "点击",
            buy_first_10['点击'].tolist(),
            yaxis_index=1,
        )
            .extend_axis(yaxis=opts.AxisOpts(name='点击次数', type_='value', position="right", offset=40))
            .set_global_opts(title_opts=opts.TitleOpts(title="支付数前10的用户，在点击、收藏、加入购物车的变化趋势"),
                             tooltip_opts=opts.TooltipOpts(trigger="axis"),
                             toolbox_opts=opts.ToolboxOpts(is_show=True),
                             xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=True, name='id'),
                             yaxis_opts=opts.AxisOpts(type_='value', name='次数', offset=20)
                             )
            .render("buy_first_10.html")
    )

    # plt.figure(figsize=(16, 10))
    # plt.subplot(311)
    # plt.plot(buy_first_10["点击"], c="r")
    # plt.title("点击数的变化趋势")
    # plt.subplot(312)
    # plt.plot(buy_first_10["收藏"], c="g")
    # plt.title("收藏数的变化趋势")
    # plt.subplot(313)
    # plt.plot(buy_first_10["加入购物车"], c="b")
    # plt.title("加入购物车的变化趋势")
    #
    # plt.xticks(np.arange(10), buy_first_10.index)
    #
    # plt.tight_layout()
    # plt.savefig("支付数前10的用户，在点击、收藏、加入购物车的变化趋势", dpi=300)
    # plt.show()




    total_custome = df[df['behavior_type'] == "4"].groupby(["date", "user_id"])["behavior_type"].count() \
        .reset_index().rename(columns={"behavior_type": "total"})
    total_custome.head()
    total_custome2 = total_custome.groupby("date").sum()["total"] / \
                     total_custome.groupby("date").count()["total"]
    total_custome2.head(10)
    # # 绘图如下
    # x = len(total_custome2.index.astype(str))
    # y = total_custome2.index.astype(str)
    #
    # plt.plot(total_custome2.values)
    # plt.xticks(range(0, 30, 7), [y[i] for i in range(0, x, 7)], rotation=90)
    # plt.title("每天的人均消费次数")
    #
    # plt.tight_layout()
    # plt.savefig("每天的人均消费次数", dpi=300)
    # plt.show()

    df["operation"] = 1
    aa = df.groupby(["date", "user_id", 'behavior_type'])["operation"].count(). \
        reset_index().rename(columns={"operation": "total"})
    aa.head(10)
    aa1 = aa.groupby("date").apply(lambda x: x[x["behavior_type"] == "4"]["total"].sum() / x["user_id"].nunique())
    aa1.head(10)
    # # 绘图如下
    # x = len(aa1.index.astype(str))
    # y = aa1.index.astype(str)
    #
    # plt.plot(aa1.values)
    # plt.xticks(range(0, 30, 7), [y[i] for i in range(0, x, 7)], rotation=90)
    # plt.title("每天的活跃用户消费次数")
    #
    # plt.tight_layout()
    # plt.savefig("每天的活跃用户消费次数", dpi=300)
    # plt.show()




    rate = aa.groupby("date").apply(lambda x: x[x["behavior_type"] == "4"]["total"].count() / x["user_id"].nunique())
    rate.head(10)
    # # 绘图如下
    # x = len(rate.index.astype(str))
    # y = rate.index.astype(str)
    #
    # plt.plot(rate.values)
    # plt.xticks(range(0, 30, 7), [y[i] for i in range(0, x, 7)], rotation=90)
    # plt.title("付费率分析")
    #
    # plt.tight_layout()
    # plt.savefig("付费率分析", dpi=300)
    # plt.show()




    re_buy = df[df["behavior_type"] == "4"].groupby("user_id")["date"].apply(lambda x: x.nunique())
    print(len(re_buy))
    re_buy[re_buy >= 2].count() / (re_buy.count() + 0.000000005)



def process3():
    df_count = df.groupby("behavior_type").size().reset_index().rename(columns={"behavior_type": "环节", 0: "人数"})

    type_dict = {
        "1": "点击",
        "2": "收藏",
        "3": "加入购物车",
        "4": "支付"
    }
    df_count["环节"] = df_count["环节"].map(type_dict)

    a = df_count.iloc[0]["人数"]
    b = df_count.iloc[1]["人数"]
    c = df_count.iloc[2]["人数"]
    d = df_count.iloc[3]["人数"]
    funnel = pd.DataFrame({"环节": ["点击", "收藏及加入购物车", "支付"], "人数": [a, b + c, d]})

    funnel["总体转化率"] = [round(i / funnel["人数"][0] * 100, 4) for i in funnel["人数"]]
    funnel["单一转化率"] = np.array([1.0, 2.0, 3.0])

    for i in range(0, len(funnel["人数"])):
        if i == 0:
            funnel["单一转化率"][i] = 1.0
        else:
            funnel["单一转化率"][i] = funnel["人数"][i] / funnel["人数"][i - 1]

    # funnel['tmp'] = funnel['人数'].map(str) + "  总体转化率：" + funnel['总体转化率'].map(str) + "% "
    # print(funnel['tmp'])
    # from pyecharts.charts import Funnel
    # c = (
    #     Funnel()
    #         .add(
    #         "商品",
    #         [list(z) for z in zip(funnel["环节"].tolist(), funnel['tmp'].tolist())],
    #         label_opts=opts.LabelOpts(position="inside"),
    #         tooltip_opts=opts.TooltipOpts(position='right'),
    #     )
    #         .set_global_opts(title_opts=opts.TitleOpts(title="Funnel-Label（inside)"))
    #         .render("funnel.html")
    # )

    # 绘图如下
    import plotly
    import plotly.graph_objs as go
    trace = go.Funnel(
        y=["点击", "收藏及加入购物车", "购买"],
        x=[funnel["人数"][0], funnel["人数"][1], funnel["人数"][2]],
        textinfo="value+percent initial",
        marker=dict(color=["deepskyblue", "lightsalmon", "tan"]),
        connector={"line": {"color": "royalblue", "dash": "solid", "width": 3}})

    data = [trace]

    fig = go.Figure(data)
    plotly.offline.plot(fig, filename="funnel.html")


def process4():
    from datetime import datetime
    # 最近一次购买距离现在的天数

    behavior_type_4 = spark.sql("SELECT * FROM data where behavior_type = '4'")
    behavior_type_4 = to_Pandas(behavior_type_4)
    print(behavior_type_4)
    recent_buy = behavior_type_4.groupby("user_id")["date"].\
        apply(lambda x: datetime(2014, 12, 20) - datetime.strptime(x.sort_values().iloc[-1], "%Y-%m-%d")).reset_index().rename(columns={"date": "recent"})
    recent_buy["recent"] = recent_buy["recent"].apply(lambda x: x.days)
    print(recent_buy[:10])
    # 购买次数计算
    buy_freq = behavior_type_4.groupby("user_id")["date"].count().reset_index(). \
        rename(columns={"date": "freq"})
    print(buy_freq[:10])
    # 将上述两列数据，合并起来
    rfm = pd.merge(recent_buy, buy_freq, on="user_id")
    print(rfm[:10])
    # 给不同类型打分
    r_bins = [0, 5, 10, 15, 20, 50]
    f_bins = [1, 30, 60, 90, 120, 900]
    rfm["r_score"] = pd.cut(rfm["recent"], bins=r_bins, labels=[5, 4, 3, 2, 1], right=False)
    rfm["f_score"] = pd.cut(rfm["freq"], bins=f_bins, labels=[1, 2, 3, 4, 5], right=False)
    for i in ["r_score", "f_score"]:
        rfm[i] = rfm[i].astype(float)
    print(rfm.describe())
    # 比较各分值与各自均值的大小
    rfm["r"] = np.where(rfm["r_score"] > 3.943957, "高", "低")
    rfm["f"] = np.where(rfm["f_score"] > 1.133356, "高", "低")
    # 将r和f列的字符串合并起来
    rfm["value"] = rfm["r"].str[:] + rfm["f"].str[:]
    print(rfm.head())

    # 自定义函数给用户贴标签
    def trans_labels(x):
        if x == "高高":
            return "重要价值客户"
        elif x == "低高":
            return "重要唤回客户"
        elif x == "高低":
            return "重要深耕客户"
        else:
            return "重要挽回客户"

    rfm["标签"] = rfm["value"].apply(trans_labels)
    # 计算出每个标签的用户数量
    print(rfm["标签"].value_counts())


# pvanduv()
# process1()
# process2()
# process3()
process4()